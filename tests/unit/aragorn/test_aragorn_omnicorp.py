"""Tests for the local aragorn_omnicorp worker.

Builds two on-disk LMDB files matching the real schema (json-encoded curie
records, raw little-endian-int shared counts) and exercises both the leaf
LMDB shims and the full overlay on a small TRAPI message.
"""

import copy
import json
import logging

import lmdb
import pytest

from shepherd_utils.config import settings
from workers.aragorn_omnicorp import worker


def _build_curies_lmdb(path, entries):
    """Write ``{curie: {"pmc": int, "index": int}}`` entries to a single-file LMDB."""
    env = lmdb.open(str(path), subdir=False, map_size=10 * 1024 * 1024)
    with env.begin(write=True) as txn:
        for curie, payload in entries.items():
            txn.put(curie.encode("utf-8"), json.dumps(payload).encode("utf-8"))
    env.close()


def _build_shared_counts_lmdb(path, entries):
    """Write ``{"<i1>_<i2>": int}`` entries as raw little-endian unsigned ints."""
    env = lmdb.open(str(path), subdir=False, map_size=10 * 1024 * 1024)
    with env.begin(write=True) as txn:
        for key, count in entries.items():
            # 8 bytes is comfortable for any plausible publication count.
            txn.put(key.encode("utf-8"), int(count).to_bytes(8, "little", signed=False))
    env.close()


@pytest.fixture
def lmdb_envs(tmp_path, monkeypatch):
    """Build fresh curie + shared-count LMDBs and point the worker at them.

    The fixture also resets the worker's cached env handles so each test
    gets a clean open against its own data; the worker normally caches one
    env handle per process.
    """
    curies_path = tmp_path / "curies.db"
    shared_counts_path = tmp_path / "shared_counts.db"

    curie_entries = {
        # CURIE -> {pmc: publication_count, index: int used to key shared counts}
        "MONDO:0001": {"pmc": 100, "index": 1},
        "CHEBI:0001": {"pmc": 50, "index": 2},
        "HP:0001": {"pmc": 25, "index": 3},
    }
    # Shared counts keyed by sorted "i1_i2" pairs (matches make_key).
    shared_count_entries = {
        "1_2": 10,  # MONDO:0001 vs CHEBI:0001
        "2_3": 5,  # CHEBI:0001 vs HP:0001
        # MONDO:0001 vs HP:0001 deliberately omitted to test the missing-key path
    }

    _build_curies_lmdb(curies_path, curie_entries)
    _build_shared_counts_lmdb(shared_counts_path, shared_count_entries)

    monkeypatch.setattr(settings, "omnicorp_curies_lmdb_path", str(curies_path))
    monkeypatch.setattr(
        settings, "omnicorp_shared_counts_lmdb_path", str(shared_counts_path)
    )
    monkeypatch.setattr(worker, "_curies_env", None)
    monkeypatch.setattr(worker, "_shared_counts_env", None)

    yield {
        "curies_path": curies_path,
        "shared_counts_path": shared_counts_path,
        "curie_entries": curie_entries,
        "shared_count_entries": shared_count_entries,
    }

    # Close any envs the worker opened during the test so the next test
    # opens its own copy against a fresh tmp_path.
    if worker._curies_env is not None:
        worker._curies_env.close()
    if worker._shared_counts_env is not None:
        worker._shared_counts_env.close()


def test_curie_query_decodes_present_and_missing(lmdb_envs):
    result = worker.curie_query(["MONDO:0001", "CHEBI:0001", "DOES:NOT_EXIST"])

    assert result["MONDO:0001"] == {"pmc": 100, "index": 1}
    assert result["CHEBI:0001"] == {"pmc": 50, "index": 2}
    # Missing curies return {} so the upstream `if len(result) == 0` check works.
    assert result["DOES:NOT_EXIST"] == {}


def test_shared_count_query_decodes_int_bytes_and_missing(lmdb_envs):
    result = worker.shared_count_query(["1_2", "2_3", "9_99"])

    assert result["1_2"] == 10
    assert result["2_3"] == 5
    # Missing pair keys come back as None so the caller can skip them.
    assert result["9_99"] is None


def test_make_key_sorts_indices():
    assert worker.make_key(("a", "b"), {"a": 5, "b": 2}) == "2_5"
    assert worker.make_key(("a", "b"), {"a": 2, "b": 5}) == "2_5"


def test_batches_handles_partial_final_batch():
    chunks = list(worker.batches(["a", "b", "c", "d", "e"], 2))
    assert chunks == [["a", "b"], ["c", "d"], ["e"]]


def test_omnicorp_overlay_full_path(lmdb_envs):
    """End-to-end: node counts annotated, support edges + auxgraph wired up.

    The query graph has all-BATCH set_interpretations (so no setnode logic
    fires), three nodes, and one analysis. Two of the three pairs have
    non-zero shared counts in the LMDB; one is missing.
    """
    in_message = {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"set_interpretation": "BATCH"},
                    "n1": {"set_interpretation": "BATCH"},
                    "n2": {"set_interpretation": "BATCH"},
                },
                "edges": {
                    "e0": {"subject": "n0", "object": "n1"},
                },
            },
            "knowledge_graph": {
                "nodes": {
                    "MONDO:0001": {"attributes": []},
                    "CHEBI:0001": {"attributes": []},
                    "HP:0001": {"attributes": []},
                },
                "edges": {
                    "kedge_0": {
                        "subject": "MONDO:0001",
                        "object": "CHEBI:0001",
                        "attributes": [],
                    },
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "n0": {"ids": ["MONDO:0001"]},
                        "n1": {"ids": ["CHEBI:0001"]},
                        "n2": {"ids": ["HP:0001"]},
                    },
                    "analyses": [
                        {
                            "edge_bindings": {"e0": {"ids": ["kedge_0"]}},
                        }
                    ],
                }
            ],
        }
    }

    logger = logging.getLogger(__name__)
    out = worker.omnicorp_overlay(copy.deepcopy(in_message), logger)

    nodes = out["message"]["knowledge_graph"]["nodes"]

    # Every kgraph node gets an omnicorp_article_count attribute.
    def article_count(node_id):
        attrs = nodes[node_id]["attributes"]
        article_attrs = [
            a
            for a in attrs
            if a.get("original_attribute_name") == "omnicorp_article_count"
        ]
        assert len(article_attrs) == 1
        return article_attrs[0]["value"]

    assert article_count("MONDO:0001") == 100
    assert article_count("CHEBI:0001") == 50
    assert article_count("HP:0001") == 25

    # Two pairs have non-zero shared counts, so two new
    # biolink:occurs_together_in_literature_with edges should land in the kg.
    edges = out["message"]["knowledge_graph"]["edges"]
    co_occurrence_edges = [
        e
        for e in edges.values()
        if e.get("predicate") == "biolink:occurs_together_in_literature_with"
    ]
    assert len(co_occurrence_edges) == 2

    pair_to_count = {}
    for e in co_occurrence_edges:
        pair = tuple(sorted((e["subject"], e["object"])))
        for attr in e["attributes"]:
            if attr["attribute_type_id"] == "biolink:has_count":
                pair_to_count[pair] = attr["value"]

    assert pair_to_count == {
        ("CHEBI:0001", "MONDO:0001"): 10,
        ("CHEBI:0001", "HP:0001"): 5,
    }

    # Each new edge should attribute itself to omnicorp.
    for e in co_occurrence_edges:
        sources = e["sources"]
        assert sources == [
            {
                "resource_id": "infores:omnicorp",
                "resource_role": "primary_knowledge_source",
            }
        ]

    # The single analysis should have one OMNICORP support graph attached
    # that bundles both new edges.
    analysis = out["message"]["results"][0]["analyses"][0]
    omnicorp_sg_ids = [
        sg
        for sg in analysis.get("support_graphs", [])
        if sg.startswith("OMNICORP_support_graph")
    ]
    # The analysis is referenced by every curie pair, but its OMNICORP support
    # graph must only be attached once — no matter how many pairs contribute
    # co-occurrence edges to it.
    assert len(omnicorp_sg_ids) == 1
    # And it must not appear more than once in the support_graphs list.
    assert analysis["support_graphs"].count(omnicorp_sg_ids[0]) == 1

    aux_graphs = out["message"]["auxiliary_graphs"]
    referenced = {sg for sg in omnicorp_sg_ids if sg in aux_graphs}
    assert referenced  # at least one referenced auxgraph exists

    aux_edge_ids = []
    for sg_id in referenced:
        aux_edge_ids.extend(aux_graphs[sg_id]["edges"])

    co_occurrence_edge_ids = {
        eid
        for eid, e in edges.items()
        if e.get("predicate") == "biolink:occurs_together_in_literature_with"
    }
    # Every co-occurrence edge ends up in an OMNICORP support graph.
    assert co_occurrence_edge_ids.issubset(set(aux_edge_ids))


def test_omnicorp_overlay_skips_overlay_above_pair_threshold(lmdb_envs, monkeypatch):
    """Queries at/above OMNICORP_MAX_CURIE_PAIRS return without the overlay.

    The message below yields 3 curie pairs; with the threshold lowered to 2,
    the per-pair shared-count overlay is skipped and no co-occurrence edges are
    added. (Node article counts still run before the gate and are unaffected.)
    """
    monkeypatch.setattr(worker, "OMNICORP_MAX_CURIE_PAIRS", 2)

    in_message = {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"set_interpretation": "BATCH"},
                    "n1": {"set_interpretation": "BATCH"},
                    "n2": {"set_interpretation": "BATCH"},
                },
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            },
            "knowledge_graph": {
                "nodes": {
                    "MONDO:0001": {"attributes": []},
                    "CHEBI:0001": {"attributes": []},
                    "HP:0001": {"attributes": []},
                },
                "edges": {
                    "kedge_0": {
                        "subject": "MONDO:0001",
                        "object": "CHEBI:0001",
                        "attributes": [],
                    },
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "n0": {"ids": ["MONDO:0001"]},
                        "n1": {"ids": ["CHEBI:0001"]},
                        "n2": {"ids": ["HP:0001"]},
                    },
                    "analyses": [
                        {"edge_bindings": {"e0": {"ids": ["kedge_0"]}}},
                    ],
                }
            ],
        }
    }

    logger = logging.getLogger(__name__)
    out = worker.omnicorp_overlay(copy.deepcopy(in_message), logger)

    # No co-occurrence edges should be added when the overlay is skipped.
    co_occurrence_edges = [
        e
        for e in out["message"]["knowledge_graph"]["edges"].values()
        if e.get("predicate") == "biolink:occurs_together_in_literature_with"
    ]
    assert co_occurrence_edges == []
    # And no auxiliary graphs / support graphs should be wired up. TRAPI 2.0
    # forbids an empty auxiliary_graphs object, so it must be absent.
    assert "auxiliary_graphs" not in out["message"]
    assert "support_graphs" not in out["message"]["results"][0]["analyses"][0]
    assert "logs" not in out


def test_omnicorp_overlay_skips_zero_shared_counts(lmdb_envs):
    """A pair whose shared count is 0 should not produce a co-occurrence edge."""
    # Add a zero-count pair to the shared-counts LMDB.
    env = lmdb.open(
        str(lmdb_envs["shared_counts_path"]),
        subdir=False,
        map_size=10 * 1024 * 1024,
    )
    with env.begin(write=True) as txn:
        # MONDO(1) <-> HP(3) -> "1_3"
        txn.put(b"1_3", (0).to_bytes(8, "little", signed=False))
    env.close()
    # Force the worker to reopen against the updated file.
    worker._shared_counts_env = None

    in_message = {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"set_interpretation": "BATCH"},
                    "n1": {"set_interpretation": "BATCH"},
                },
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            },
            "knowledge_graph": {
                "nodes": {
                    "MONDO:0001": {"attributes": []},
                    "HP:0001": {"attributes": []},
                },
                "edges": {
                    "kedge_0": {
                        "subject": "MONDO:0001",
                        "object": "HP:0001",
                        "attributes": [],
                    }
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "n0": {"ids": ["MONDO:0001"]},
                        "n1": {"ids": ["HP:0001"]},
                    },
                    "analyses": [
                        {"edge_bindings": {"e0": {"ids": ["kedge_0"]}}},
                    ],
                }
            ],
        }
    }

    logger = logging.getLogger(__name__)
    out = worker.omnicorp_overlay(copy.deepcopy(in_message), logger)

    co_occurrence_edges = [
        e
        for e in out["message"]["knowledge_graph"]["edges"].values()
        if e.get("predicate") == "biolink:occurs_together_in_literature_with"
    ]
    assert co_occurrence_edges == []


def test_aragorn_omnicorp_loads_overlays_saves_and_preserves_workflow(
    lmdb_envs, monkeypatch
):
    """The process-pool entrypoint reads by id, overlays, and writes back.

    ``aragorn_omnicorp`` is the function dispatched to the process pool: only
    the ``response_id`` crosses the boundary. It loads the message from Redis via
    ``get_message_sync``, applies the overlay, and persists it with
    ``save_message_sync`` -- the large payload never has to be passed in or
    returned across the process boundary. It must also strip and restore any
    top-level ``workflow`` around the overlay.
    """
    loaded = {
        "workflow": {"ids": ["aragorn.omnicorp"]},
        "message": {
            "query_graph": {
                "nodes": {"n0": {"set_interpretation": "BATCH"}},
                "edges": {},
            },
            "knowledge_graph": {
                "nodes": {"MONDO:0001": {"attributes": []}},
                "edges": {},
            },
            "results": [],
        },
    }

    saved = {}
    monkeypatch.setattr(
        worker, "get_message_sync", lambda response_id: copy.deepcopy(loaded)
    )
    monkeypatch.setattr(
        worker,
        "save_message_sync",
        lambda response_id, message: saved.update({response_id: message}),
    )

    logger = logging.getLogger(__name__)
    worker.aragorn_omnicorp("resp-1", logger)

    # The overlaid message was written back under its id.
    assert "resp-1" in saved
    out = saved["resp-1"]

    # The workflow is stripped before the overlay and restored afterwards.
    assert out["workflow"] == {"ids": ["aragorn.omnicorp"]}

    node = out["message"]["knowledge_graph"]["nodes"]["MONDO:0001"]
    article_attrs = [
        a
        for a in node["attributes"]
        if a.get("original_attribute_name") == "omnicorp_article_count"
    ]
    assert article_attrs and article_attrs[0]["value"] == 100


def test_already_overlaid_detects_node_annotation():
    assert worker._already_overlaid({"nodes": {}}) is False
    assert worker._already_overlaid({"nodes": {"n": {"attributes": []}}}) is False
    assert worker._already_overlaid({"nodes": {"n": {"attributes": None}}}) is False
    assert (
        worker._already_overlaid(
            {
                "nodes": {
                    "n": {
                        "attributes": [
                            {
                                "original_attribute_name": "omnicorp_article_count",
                                "value": 5,
                            }
                        ]
                    }
                }
            }
        )
        is True
    )


def test_omnicorp_overlay_is_idempotent_on_rerun(lmdb_envs):
    """Re-running the overlay on an already-overlaid message is a no-op.

    Guards the reclaim-redelivery case (a prior run saved the overlaid message
    but died before ACKing): without the guard a second pass would double node
    counts and append duplicate co-occurrence edges.
    """
    in_message = {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"set_interpretation": "BATCH"},
                    "n1": {"set_interpretation": "BATCH"},
                    "n2": {"set_interpretation": "BATCH"},
                },
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            },
            "knowledge_graph": {
                "nodes": {
                    "MONDO:0001": {"attributes": []},
                    "CHEBI:0001": {"attributes": []},
                    "HP:0001": {"attributes": []},
                },
                "edges": {
                    "kedge_0": {
                        "subject": "MONDO:0001",
                        "object": "CHEBI:0001",
                        "attributes": [],
                    },
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "n0": {"ids": ["MONDO:0001"]},
                        "n1": {"ids": ["CHEBI:0001"]},
                        "n2": {"ids": ["HP:0001"]},
                    },
                    "analyses": [{"edge_bindings": {"e0": {"ids": ["kedge_0"]}}}],
                }
            ],
        }
    }

    logger = logging.getLogger(__name__)
    once = worker.omnicorp_overlay(copy.deepcopy(in_message), logger)
    # Feed the already-overlaid output back through, as a reclaim would.
    twice = worker.omnicorp_overlay(copy.deepcopy(once), logger)

    def article_counts(msg, node_id):
        return [
            a
            for a in msg["message"]["knowledge_graph"]["nodes"][node_id]["attributes"]
            if a.get("original_attribute_name") == "omnicorp_article_count"
        ]

    def cooccurrence_edges(msg):
        return [
            e
            for e in msg["message"]["knowledge_graph"]["edges"].values()
            if e.get("predicate") == "biolink:occurs_together_in_literature_with"
        ]

    # Node counts are not doubled and no duplicate edges were appended.
    for node_id in ("MONDO:0001", "CHEBI:0001", "HP:0001"):
        assert len(article_counts(twice, node_id)) == 1
    assert len(cooccurrence_edges(twice)) == len(cooccurrence_edges(once))


def test_generate_curie_pairs_stops_at_max_pairs():
    """``max_pairs`` bounds the mapping instead of materializing every pair."""
    from workers.aragorn_omnicorp.worker import generate_curie_pairs

    # One analysis over 50 nonset nodes => C(50, 2) = 1225 candidate pairs.
    node_ids = [f"N{i}" for i in range(50)]
    answers = [
        {
            "node_bindings": {"qother": {"ids": node_ids}},
            "analyses": [{"edge_bindings": {}}],
        }
    ]
    node_pub_counts = {nid: 1 for nid in node_ids}
    message = {"knowledge_graph": {"edges": {}}, "auxiliary_graphs": {}}
    logger = logging.getLogger(__name__)

    # Uncapped materializes all 1225; capped stops at the threshold.
    full = generate_curie_pairs(answers, set(), node_pub_counts, message, logger)
    assert len(full) == 1225

    capped = generate_curie_pairs(
        answers, set(), node_pub_counts, message, logger, max_pairs=100
    )
    assert len(capped) == 100


def _valid_2_0_message():
    """A small, schema-valid TRAPI 2.0 response for the overlay."""
    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"ids": ["MONDO:0001"]},
                    "n1": {"categories": ["biolink:ChemicalEntity"]},
                    "n2": {"categories": ["biolink:PhenotypicFeature"]},
                },
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            },
            "knowledge_graph": {
                "nodes": {
                    "MONDO:0001": {"categories": ["biolink:Disease"]},
                    "CHEBI:0001": {"categories": ["biolink:ChemicalEntity"]},
                    "HP:0001": {"categories": ["biolink:PhenotypicFeature"]},
                },
                "edges": {
                    "kedge_0": {
                        "subject": "MONDO:0001",
                        "predicate": "biolink:related_to",
                        "object": "CHEBI:0001",
                        "knowledge_level": "knowledge_assertion",
                        "agent_type": "manual_agent",
                        "sources": [
                            {
                                "resource_id": "infores:test",
                                "resource_role": "primary_knowledge_source",
                            }
                        ],
                    },
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "n0": {"ids": ["MONDO:0001"]},
                        "n1": {"ids": ["CHEBI:0001"]},
                        "n2": {"ids": ["HP:0001"]},
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:test",
                            "edge_bindings": {"e0": {"ids": ["kedge_0"]}},
                        }
                    ],
                }
            ],
        }
    }


def test_omnicorp_overlay_output_is_valid_trapi_2(lmdb_envs):
    """The overlaid message validates as TRAPI 2.0: co-occurrence edges carry
    knowledge_level / agent_type as top-level properties (not attributes) and
    auxiliary graphs are ``{"edges": [...]}`` only."""
    from translator_tom import Response

    out = worker.omnicorp_overlay(_valid_2_0_message(), logging.getLogger(__name__))
    Response.from_dict(out)

    co_occurrence = [
        e
        for e in out["message"]["knowledge_graph"]["edges"].values()
        if e["predicate"] == "biolink:occurs_together_in_literature_with"
    ]
    assert len(co_occurrence) == 2
    for edge in co_occurrence:
        assert edge["knowledge_level"] == "statistical_association"
        assert edge["agent_type"] == "statistical_association_pipeline"
        assert {a["attribute_type_id"] for a in edge["attributes"]} == {
            "biolink:has_count"
        }
    for aux_graph in out["message"]["auxiliary_graphs"].values():
        assert set(aux_graph) == {"edges"}
    assert "logs" not in out


def test_omnicorp_overlay_tolerates_result_without_analyses(lmdb_envs):
    """TRAPI 2.0: Result.analyses is optional."""
    msg = _valid_2_0_message()
    del msg["message"]["results"][0]["analyses"]
    out = worker.omnicorp_overlay(msg, logging.getLogger(__name__))
    assert "analyses" not in out["message"]["results"][0]
    assert "auxiliary_graphs" not in out["message"]


def test_create_log_entry_is_trapi_2_log_entry():
    from translator_tom import LogEntry

    entry = worker.create_log_entry("hello", "DEBUG")
    assert "code" not in entry
    LogEntry.from_dict(entry)
    assert worker.create_log_entry("x", "ERROR", code="E1")["code"] == "E1"
