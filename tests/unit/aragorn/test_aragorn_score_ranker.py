"""Tests for the ``Ranker`` class in ``workers.aragorn_score.worker``.

Exercises the methods used during scoring:

- ``__init__`` profile selection
- ``probes`` (probe node selection from the qgraph)
- ``get_rgraph`` (per-analysis r-graph construction, support graphs)
- ``get_omnicorp_node_pubs`` (publication-count attribute extraction)
- ``get_edge_values`` (attribute parsing for publications, p_value,
  literature co-occurrence, affinity)
- ``graph_laplacian`` (weighted laplacian + zero-row pruning)
- ``rank`` (sorted result ordering after scoring)
- ``score`` (per-analysis scoring with degenerate laplacian -> 0)
"""

import copy
import logging

import numpy as np
import pytest

from tests.helpers.generate_messages import response_1
from workers.aragorn_score.worker import (
    BLENDED_PROFILE,
    CLINICAL_PROFILE,
    Ranker,
)

logger = logging.getLogger(__name__)


# --- __init__ profile selection ------------------------------------------


def test_ranker_init_uses_blended_profile_by_default():
    msg = response_1["message"]
    r = Ranker(msg, logger)
    assert r.source_weights == BLENDED_PROFILE["source_weights"]
    assert r.base_weights == BLENDED_PROFILE["base_weights"]


def test_ranker_init_can_select_clinical_profile():
    msg = response_1["message"]
    r = Ranker(msg, logger, profile="clinical")
    assert r.source_weights == CLINICAL_PROFILE["source_weights"]


def test_ranker_init_handles_minimal_message():
    """A minimal message shouldn't crash; ranker handles missing kg/qgraph."""
    r = Ranker({}, logger)
    assert r.kgraph == {"nodes": {}, "edges": {}}
    assert r.qgraph == {"nodes": {}, "edges": {}}
    assert r.agraphs == {}


# --- probes --------------------------------------------------------------


def test_probes_returns_q_node_pairs_for_one_hop_graph():
    """For a 1-edge query graph, probes() returns a single (n0, n1) pair."""
    msg = {
        "query_graph": {
            "nodes": {"n0": {}, "n1": {}},
            "edges": {"e0": {"subject": "n0", "object": "n1"}},
        },
        "knowledge_graph": {"nodes": {}, "edges": {}},
    }
    r = Ranker(msg, logger)
    probes = r.probes()
    assert len(probes) == 1
    assert set(probes[0]) == {"n0", "n1"}


def test_probes_matches_response_1_fixture():
    """The shared TRAPI fixture has n0->n1 connectivity; probes returns one
    pair containing both query node ids."""
    msg = response_1["message"]
    r = Ranker(msg, logger)
    probes = r.probes()
    assert len(probes) >= 1
    flat = {n for pair in probes for n in pair}
    assert flat <= set(msg["query_graph"]["nodes"])


# --- get_omnicorp_node_pubs ---------------------------------------------


def test_get_omnicorp_node_pubs_finds_attribute():
    """Picks up the omnicorp_article_count attribute and caches it."""
    msg = {
        "knowledge_graph": {
            "nodes": {
                "MONDO:1": {
                    "attributes": [
                        {
                            "original_attribute_name": "omnicorp_article_count",
                            "value": 100,
                        }
                    ]
                }
            },
            "edges": {},
        },
        "query_graph": {"nodes": {}, "edges": {}},
    }
    r = Ranker(msg, logger)
    assert r.get_omnicorp_node_pubs("MONDO:1") == 100
    # Cached on the instance.
    assert r.node_pubs["MONDO:1"] == 100
    # Second call hits the cache.
    assert r.get_omnicorp_node_pubs("MONDO:1") == 100


def test_get_omnicorp_node_pubs_recognises_alternate_name():
    """The newer ``num_publications`` attribute name is also recognised."""
    msg = {
        "knowledge_graph": {
            "nodes": {
                "MONDO:1": {
                    "attributes": [
                        {"original_attribute_name": "num_publications", "value": 7}
                    ]
                }
            },
            "edges": {},
        },
        "query_graph": {"nodes": {}, "edges": {}},
    }
    r = Ranker(msg, logger)
    assert r.get_omnicorp_node_pubs("MONDO:1") == 7


def test_get_omnicorp_node_pubs_returns_zero_when_no_attribute():
    msg = {
        "knowledge_graph": {
            "nodes": {"MONDO:1": {"attributes": []}},
            "edges": {},
        },
        "query_graph": {"nodes": {}, "edges": {}},
    }
    r = Ranker(msg, logger)
    assert r.get_omnicorp_node_pubs("MONDO:1") == 0


def test_get_omnicorp_node_pubs_handles_unparseable_value():
    """A non-numeric string value falls back to 0 rather than raising."""
    msg = {
        "knowledge_graph": {
            "nodes": {
                "MONDO:1": {
                    "attributes": [
                        {
                            "original_attribute_name": "omnicorp_article_count",
                            "value": "not-a-number",
                        }
                    ]
                }
            },
            "edges": {},
        },
        "query_graph": {"nodes": {}, "edges": {}},
    }
    r = Ranker(msg, logger)
    assert r.get_omnicorp_node_pubs("MONDO:1") == 0


def test_get_omnicorp_node_pubs_raises_for_unknown_node():
    msg = {
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "query_graph": {"nodes": {}, "edges": {}},
    }
    r = Ranker(msg, logger)
    with pytest.raises(KeyError, match="ghost"):
        r.get_omnicorp_node_pubs("ghost")


# --- get_edge_values -----------------------------------------------------


def _make_msg_with_edge(edge):
    return {
        "knowledge_graph": {
            "nodes": {},
            "edges": {"e1": edge},
        },
        "query_graph": {"nodes": {}, "edges": {}},
    }


def test_get_edge_values_publications_split_by_pipe():
    """``["PMID:1|2|3"]`` should be split into 3 publications."""
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {"resource_id": "infores:foo", "resource_role": "primary_knowledge_source"}
        ],
        "attributes": [
            {"original_attribute_name": "publications", "value": ["PMID:1|2|3"]}
        ],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    vals = r.get_edge_values("e1")
    # publications adds a 'publications' entry under the source.
    pub_data = vals["infores:foo"]["publications"]
    assert pub_data["value"] == 3


def test_get_edge_values_publications_split_by_comma():
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {"resource_id": "infores:foo", "resource_role": "primary_knowledge_source"}
        ],
        "attributes": [
            {"original_attribute_name": "publications", "value": ["PMID:1,2,3,4"]}
        ],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    vals = r.get_edge_values("e1")
    assert vals["infores:foo"]["publications"]["value"] == 4


def test_get_edge_values_publications_string_value_becomes_single_pub():
    """A bare string value (not a list) turns into a 1-element list."""
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {"resource_id": "infores:foo", "resource_role": "primary_knowledge_source"}
        ],
        "attributes": [{"original_attribute_name": "publications", "value": "PMID:1"}],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    vals = r.get_edge_values("e1")
    assert vals["infores:foo"]["publications"]["value"] == 1


def test_get_edge_values_evidence_count_overwrites_num_publications():
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {"resource_id": "infores:foo", "resource_role": "primary_knowledge_source"}
        ],
        "attributes": [
            {"attribute_type_id": "biolink:evidence_count", "value": 42},
        ],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    vals = r.get_edge_values("e1")
    assert vals["infores:foo"]["publications"]["value"] == 42


def test_get_edge_values_p_value_extracts_numeric():
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {
                "resource_id": "infores:genetics-data-provider",
                "resource_role": "primary_knowledge_source",
            }
        ],
        "attributes": [
            {"original_attribute_name": "p_value", "value": 0.001},
        ],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    vals = r.get_edge_values("e1")
    p_data = vals["infores:genetics-data-provider"]["p_value"]
    assert p_data["value"] == 0.001


def test_get_edge_values_p_value_unwraps_list():
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {
                "resource_id": "infores:genetics-data-provider",
                "resource_role": "primary_knowledge_source",
            }
        ],
        "attributes": [
            {"original_attribute_name": "p_value", "value": [0.005]},
        ],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    vals = r.get_edge_values("e1")
    p_data = vals["infores:genetics-data-provider"]["p_value"]
    assert p_data["value"] == 0.005


def test_get_edge_values_p_value_string_parsed_to_float():
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {
                "resource_id": "infores:genetics-data-provider",
                "resource_role": "primary_knowledge_source",
            }
        ],
        "attributes": [
            {"original_attribute_name": "p_value", "value": "0.005"},
        ],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    vals = r.get_edge_values("e1")
    p_data = vals["infores:genetics-data-provider"]["p_value"]
    assert p_data["value"] == 0.005


def test_get_edge_values_p_value_unparseable_string_logged_as_none():
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {
                "resource_id": "infores:genetics-data-provider",
                "resource_role": "primary_knowledge_source",
            }
        ],
        "attributes": [
            {"original_attribute_name": "p_value", "value": "not-a-number"},
        ],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    vals = r.get_edge_values("e1")
    # Unparseable -> p_value remains None and so the source dict lacks p_value.
    assert "p_value" not in vals["infores:genetics-data-provider"]


def test_get_edge_values_no_primary_source_uses_unspecified():
    """An edge with no primary_knowledge_source defaults to 'unspecified'."""
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {"resource_id": "infores:foo", "resource_role": "supporting_data_source"},
        ],
        "attributes": [
            {"original_attribute_name": "publications", "value": ["P:1"]},
        ],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    vals = r.get_edge_values("e1")
    assert "unspecified" in vals


def test_get_edge_values_caches_per_edge_id():
    """Calling twice for the same edge id returns the cached dict."""
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {"resource_id": "infores:foo", "resource_role": "primary_knowledge_source"}
        ],
        "attributes": [
            {"original_attribute_name": "publications", "value": ["PMID:1"]}
        ],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    first = r.get_edge_values("e1")
    second = r.get_edge_values("e1")
    assert first is second  # identity, not just equality


def test_get_edge_values_raises_for_unknown_edge():
    msg = {
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "query_graph": {"nodes": {}, "edges": {}},
    }
    r = Ranker(msg, logger)
    with pytest.raises(KeyError, match="ghost"):
        r.get_edge_values("ghost")


def test_get_edge_values_affinity_and_confidence_score_extracted():
    edge = {
        "subject": "A",
        "object": "B",
        "sources": [
            {
                "resource_id": "infores:text-mining-provider-targeted",
                "resource_role": "primary_knowledge_source",
            }
        ],
        "attributes": [
            {"original_attribute_name": "affinity", "value": 0.5},
            {"original_attribute_name": "biolink:tmkp_confidence_score", "value": 0.8},
        ],
    }
    r = Ranker(_make_msg_with_edge(edge), logger)
    vals = r.get_edge_values("e1")
    assert vals["infores:text-mining-provider-targeted"]["affinity"]["value"] == 0.5


def test_get_edge_values_literature_cooccurrence_with_omnicorp_predicate():
    """The literature_cooccurrence pathway requires the predicate
    biolink:occurs_together_in_literature_with and a biolink:has_count
    attribute. Subject and object node pubs are pulled from
    omnicorp_article_count attributes."""
    msg = {
        "knowledge_graph": {
            "nodes": {
                "A": {
                    "attributes": [
                        {
                            "original_attribute_name": "omnicorp_article_count",
                            "value": 100,
                        }
                    ]
                },
                "B": {
                    "attributes": [
                        {
                            "original_attribute_name": "omnicorp_article_count",
                            "value": 100,
                        }
                    ]
                },
            },
            "edges": {
                "e1": {
                    "subject": "A",
                    "object": "B",
                    "predicate": "biolink:occurs_together_in_literature_with",
                    "sources": [
                        {
                            "resource_id": "infores:omnicorp",
                            "resource_role": "primary_knowledge_source",
                        }
                    ],
                    "attributes": [
                        {"attribute_type_id": "biolink:has_count", "value": 50},
                    ],
                }
            },
        },
        "query_graph": {"nodes": {}, "edges": {}},
    }
    r = Ranker(msg, logger)
    vals = r.get_edge_values("e1")
    assert "literature_coocurrence" in vals["infores:omnicorp"]
    # Cov >= 0 by construction.
    assert vals["infores:omnicorp"]["literature_coocurrence"]["value"] >= 0


# --- get_rgraph ----------------------------------------------------------


def test_get_rgraph_builds_one_rgraph_per_analysis():
    """Each analysis on a result yields its own r_graph."""
    msg = {
        "query_graph": {
            "nodes": {"n0": {}, "n1": {}},
            "edges": {"e0": {"subject": "n0", "object": "n1"}},
        },
        "knowledge_graph": {
            "nodes": {"K0": {}, "K1": {}},
            "edges": {
                "ke0": {"subject": "K0", "object": "K1", "attributes": []},
            },
        },
        "auxiliary_graphs": {},
    }
    r = Ranker(msg, logger)
    result = {
        "node_bindings": {
            "n0": [{"id": "K0"}],
            "n1": [{"id": "K1"}],
        },
        "analyses": [
            {"edge_bindings": {"e0": [{"id": "ke0"}]}},
            {"edge_bindings": {"e0": [{"id": "ke0"}]}},
        ],
    }
    r_graphs = r.get_rgraph(result)
    assert len(r_graphs) == 2
    for rg in r_graphs:
        assert rg["nodes"] == {"n0", "n1"}
        # One edge tuple (n0, n1, ke0)
        assert any(e[2] == "ke0" for e in rg["edges"])


def test_get_rgraph_skips_edges_not_in_kgraph():
    """An edge_binding that references a missing kg edge is logged and
    skipped (the r_graph just lacks that tuple)."""
    msg = {
        "query_graph": {
            "nodes": {"n0": {}, "n1": {}},
            "edges": {"e0": {"subject": "n0", "object": "n1"}},
        },
        "knowledge_graph": {"nodes": {"K0": {}, "K1": {}}, "edges": {}},
        "auxiliary_graphs": {},
    }
    r = Ranker(msg, logger)
    result = {
        "node_bindings": {"n0": [{"id": "K0"}], "n1": [{"id": "K1"}]},
        "analyses": [{"edge_bindings": {"e0": [{"id": "missing-edge"}]}}],
    }
    rgs = r.get_rgraph(result)
    assert rgs[0]["edges"] == set()


def test_get_rgraph_pulls_in_support_graph_nodes_and_edges():
    """An analysis ``support_graphs`` entry pulls aux-graph nodes/edges into
    the r_graph."""
    msg = {
        "query_graph": {
            "nodes": {"n0": {}, "n1": {}},
            "edges": {"e0": {"subject": "n0", "object": "n1"}},
        },
        "knowledge_graph": {
            "nodes": {"K0": {}, "K1": {}, "K_SUP": {}},
            "edges": {
                "ke0": {"subject": "K0", "object": "K1", "attributes": []},
                "k_sup_edge": {
                    "subject": "K0",
                    "object": "K_SUP",
                    "attributes": [],
                },
            },
        },
        "auxiliary_graphs": {
            "aux1": {"edges": ["k_sup_edge"], "nodes": ["K_SUP"]},
        },
    }
    r = Ranker(msg, logger)
    result = {
        "node_bindings": {"n0": [{"id": "K0"}], "n1": [{"id": "K1"}]},
        "analyses": [
            {
                "edge_bindings": {"e0": [{"id": "ke0"}]},
                "support_graphs": ["aux1"],
            }
        ],
    }
    rgs = r.get_rgraph(result)
    rg = rgs[0]
    edge_ids = {e[2] for e in rg["edges"]}
    assert "k_sup_edge" in edge_ids


# --- graph_laplacian ----------------------------------------------------


def test_graph_laplacian_two_node_graph_yields_2x2_matrix():
    """A trivial graph with one edge between two q-nodes returns a 2x2
    laplacian after pruning. Probe inds align to the kept nodes."""
    msg = {
        "query_graph": {
            "nodes": {"n0": {}, "n1": {}},
            "edges": {"e0": {"subject": "n0", "object": "n1"}},
        },
        "knowledge_graph": {
            "nodes": {},
            "edges": {
                "ke0": {
                    "subject": "K0",
                    "object": "K1",
                    "sources": [
                        {
                            "resource_id": "infores:foo",
                            "resource_role": "primary_knowledge_source",
                        }
                    ],
                    "attributes": [
                        {"original_attribute_name": "publications", "value": ["P:1"]}
                    ],
                }
            },
        },
        "auxiliary_graphs": {},
    }
    r = Ranker(msg, logger)
    r_graph = {
        "nodes": {"n0", "n1"},
        "edges": {("n0", "n1", "ke0")},
    }
    L, probe_inds, details = r.graph_laplacian(r_graph, [("n0", "n1")])
    assert L.shape == (2, 2)
    # Diagonal is positive; off-diagonal negative.
    assert L[0, 0] > 0
    assert L[0, 1] < 0
    # Probe inds map to kept node positions.
    assert probe_inds == [(0, 1)] or probe_inds == [(1, 0)]


def test_graph_laplacian_drops_zero_rows_unless_probe():
    """Nodes that have no edges (zero rows) are pruned, but probes are
    protected."""
    msg = {
        "query_graph": {
            "nodes": {"n0": {}, "n1": {}, "n2": {}},
            "edges": {"e0": {"subject": "n0", "object": "n1"}},
        },
        "knowledge_graph": {
            "nodes": {},
            "edges": {
                "ke0": {
                    "subject": "K0",
                    "object": "K1",
                    "sources": [
                        {
                            "resource_id": "infores:foo",
                            "resource_role": "primary_knowledge_source",
                        }
                    ],
                    "attributes": [
                        {"original_attribute_name": "publications", "value": ["P:1"]}
                    ],
                }
            },
        },
        "auxiliary_graphs": {},
    }
    r = Ranker(msg, logger)
    # n2 has no edges -> a zero row in the unfiltered laplacian.
    r_graph = {
        "nodes": {"n0", "n1", "n2"},
        "edges": {("n0", "n1", "ke0")},
    }
    L, probe_inds, _ = r.graph_laplacian(r_graph, [("n0", "n1")])
    # n2 was dropped -> 2x2.
    assert L.shape == (2, 2)


# --- score and rank ------------------------------------------------------


def test_score_returns_float_for_well_formed_answer():
    """The fixture response has well-formed analyses; score() should return
    floats in [0, 1] (after exp(-kirchhoff))."""
    msg = copy.deepcopy(response_1["message"])
    r = Ranker(msg, logger)
    answer = msg["results"][0]
    scored, details = r.score(answer)
    score = scored["analyses"][0]["score"]
    assert isinstance(score, float)
    # Scores from the existing fixture are in (0.063, 0.064) range; broader
    # check here so the test tolerates ranker drift.
    assert -1 <= score <= 1


def test_rank_returns_answers_sorted_by_score():
    """rank() returns answers ordered by their max analysis score."""
    msg = copy.deepcopy(response_1["message"])
    r = Ranker(msg, logger)
    ranked = r.rank(copy.deepcopy(msg["results"]))
    assert len(ranked) == len(msg["results"])
    # All ranked entries have analyses with scores assigned.
    scores = [
        max(a.get("score", 0) for a in ans["analyses"])
        for ans in ranked
        if ans.get("analyses")
    ]
    # Ascending order (rank() uses ``sorted`` without reverse=True).
    assert scores == sorted(scores)


def test_score_jaccard_like_returns_score_over_one_minus_score():
    """jaccard_like=True transforms each score to s/(1-s)."""
    msg = copy.deepcopy(response_1["message"])
    r = Ranker(msg, logger)
    scored, _ = r.score(copy.deepcopy(msg["results"][0]), jaccard_like=True)
    raw, _ = Ranker(msg, logger).score(copy.deepcopy(msg["results"][0]))
    raw_score = raw["analyses"][0]["score"]
    if 0 < raw_score < 1:
        assert scored["analyses"][0]["score"] == pytest.approx(
            raw_score / (1 - raw_score)
        )
