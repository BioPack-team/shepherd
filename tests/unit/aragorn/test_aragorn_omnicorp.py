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
                        "n0": [{"id": "MONDO:0001"}],
                        "n1": [{"id": "CHEBI:0001"}],
                        "n2": [{"id": "HP:0001"}],
                    },
                    "analyses": [
                        {
                            "edge_bindings": {"e0": [{"id": "kedge_0"}]},
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
    # The same OMNICORP graph is appended once per pair, but the worker
    # reuses the existing one after the first match — so the analysis ends
    # up with at least one OMNICORP entry.
    assert len(omnicorp_sg_ids) >= 1

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
                        "n0": [{"id": "MONDO:0001"}],
                        "n1": [{"id": "HP:0001"}],
                    },
                    "analyses": [
                        {"edge_bindings": {"e0": [{"id": "kedge_0"}]}},
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


def test_aragorn_omnicorp_runs_overlay_and_preserves_workflow(lmdb_envs):
    """The executor entrypoint runs the overlay on a loaded message in place.

    ``aragorn_omnicorp`` is the CPU-bound function dispatched to the process
    pool: it takes an already-loaded message, applies the overlay, and returns
    it (DB load/save are handled by ``process_task`` on the event loop). It
    must also strip and restore any top-level ``workflow`` around the overlay.
    """
    in_message = {
        "workflow": [{"id": "aragorn.omnicorp"}],
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

    logger = logging.getLogger(__name__)

    out = worker.aragorn_omnicorp(copy.deepcopy(in_message), logger)

    # The workflow is stripped before the overlay and restored afterwards.
    assert out["workflow"] == [{"id": "aragorn.omnicorp"}]

    saved_node = out["message"]["knowledge_graph"]["nodes"]["MONDO:0001"]
    article_attrs = [
        a
        for a in saved_node["attributes"]
        if a.get("original_attribute_name") == "omnicorp_article_count"
    ]
    assert article_attrs and article_attrs[0]["value"] == 100
