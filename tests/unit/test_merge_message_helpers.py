"""Tests for the pure helpers in ``workers.merge_message.worker``.

These cover:

- ``get_edgeset`` — kgraph-edge frozenset extraction from a result.
- ``create_aux_graph`` — analysis -> aux graph conversion.
- ``add_knowledge_edge`` — synthetic creative-mode kg edge construction.
- ``_normalize_query`` / ``queries_equivalent`` — query graph equivalence.
- ``has_unique_nodes`` / ``filter_repeated_nodes`` — duplicate-node filtering.
- ``get_promiscuous_qnodes`` / ``remove_promiscuous_knode_results`` /
  ``filter_promiscuous_results`` — over-popular knode pruning.
- ``get_answer_node`` — finding the unpinned qnode.
- ``group_results_by_qnode`` — grouping creative + lookup results.
- ``merge_messages`` — the top-level merge (lookup, creative, pathfinder, error).
"""

import copy
import logging

import pytest

from tests.helpers.generate_messages import (
    creative_query,
    generate_query,
    generate_response,
    response_1,
    response_2,
)
from workers.merge_message.worker import (
    add_knowledge_edge,
    create_aux_graph,
    filter_promiscuous_results,
    filter_repeated_nodes,
    get_answer_node,
    get_edgeset,
    get_promiscuous_qnodes,
    group_results_by_qnode,
    has_unique_nodes,
    merge_messages,
    queries_equivalent,
    remove_promiscuous_knode_results,
    _normalize_query,
)


logger = logging.getLogger(__name__)


def test_get_edgeset_collapses_all_edge_bindings():
    result = {
        "analyses": [
            {
                "edge_bindings": {
                    "e0": [{"id": "k1"}, {"id": "k2"}],
                    "e1": [{"id": "k3"}],
                }
            },
            {"edge_bindings": {"e2": [{"id": "k4"}]}},
        ]
    }
    out = get_edgeset(result)
    assert out == frozenset({"k1", "k2", "k3", "k4"})


def test_create_aux_graph_returns_uuid_and_edge_list():
    analysis = {
        "edge_bindings": {
            "e0": [{"id": "kedge_a"}, {"id": "kedge_b"}],
            "e1": [{"id": "kedge_c"}],
        }
    }
    aux_id, aux_graph = create_aux_graph(analysis)
    assert isinstance(aux_id, str) and len(aux_id) > 0
    assert sorted(aux_graph["edges"]) == ["kedge_a", "kedge_b", "kedge_c"]
    assert aux_graph["attributes"] == []


def test_add_knowledge_edge_with_object_pinned_uses_answer_as_subject():
    """When the object qnode is pinned (creative_query default), the new
    knowledge edge uses the answer CURIE as subject and pinned id as object."""
    result_message = copy.deepcopy(creative_query)
    result_message["message"]["knowledge_graph"] = {"nodes": {}, "edges": {}}
    new_edge_id = add_knowledge_edge(
        target="aragorn",
        result_message=result_message,
        aux_graph_ids=["aux1", "aux2"],
        answer="CHEBI:NEW",
    )
    edges = result_message["message"]["knowledge_graph"]["edges"]
    new_edge = edges[new_edge_id]
    assert new_edge["subject"] == "CHEBI:NEW"  # answer was the unpinned subject
    assert new_edge["object"] == "MONDO:0001"  # pinned object's id
    assert new_edge["predicate"] == "biolink:treats"
    # Aux graph id list lands in the support_graphs attribute.
    sg_attrs = [
        a
        for a in new_edge["attributes"]
        if a["attribute_type_id"] == "biolink:support_graphs"
    ]
    assert sg_attrs[0]["value"] == ["aux1", "aux2"]
    # Source is shepherd-{target}.
    assert new_edge["sources"][0]["resource_id"] == "infores:shepherd-aragorn"


def test_add_knowledge_edge_with_subject_pinned_uses_answer_as_object():
    """Mirror of the above: subject is pinned, answer is the object."""
    msg = copy.deepcopy(creative_query)
    msg["message"]["query_graph"]["nodes"]["SN"]["ids"] = ["CHEBI:1"]
    msg["message"]["query_graph"]["nodes"]["ON"].pop("ids")
    msg["message"]["knowledge_graph"] = {"nodes": {}, "edges": {}}
    new_edge_id = add_knowledge_edge(
        target="aragorn",
        result_message=msg,
        aux_graph_ids=["aux1"],
        answer="MONDO:NEW",
    )
    new_edge = msg["message"]["knowledge_graph"]["edges"][new_edge_id]
    assert new_edge["subject"] == "CHEBI:1"
    assert new_edge["object"] == "MONDO:NEW"


def test_add_knowledge_edge_passes_through_qualifier_constraints():
    msg = copy.deepcopy(creative_query)
    msg["message"]["query_graph"]["edges"]["e0"]["qualifier_constraints"] = [
        {
            "qualifier_set": [
                {
                    "qualifier_type_id": "biolink:object_aspect_qualifier",
                    "qualifier_value": "activity",
                }
            ]
        }
    ]
    msg["message"]["knowledge_graph"] = {"nodes": {}, "edges": {}}
    new_edge_id = add_knowledge_edge(
        target="aragorn", result_message=msg, aux_graph_ids=["a"], answer="CHEBI:NEW"
    )
    new_edge = msg["message"]["knowledge_graph"]["edges"][new_edge_id]
    assert new_edge["qualifiers"] == [
        {
            "qualifier_type_id": "biolink:object_aspect_qualifier",
            "qualifier_value": "activity",
        }
    ]


def test_normalize_query_collapses_optional_and_synonym_predicates():
    """Empty constraint lists, BATCH set_interpretation, and biolink:treats →
    biolink:treats_or_applied_or_studied_to_treat all get normalized away."""
    q = {
        "nodes": {
            "n": {
                "ids": None,
                "categories": None,
                "is_set": False,
                "set_interpretation": "BATCH",
                "constraints": [],
                "member_ids": [],
            }
        },
        "edges": {
            "e": {
                "subject": "n",
                "object": "n",
                "predicates": ["biolink:treats"],
                "knowledge_type": "lookup",
                "attribute_constraints": [],
                "qualifier_constraints": [],
            }
        },
    }
    out = _normalize_query(q)
    n = out["nodes"]["n"]
    assert "ids" not in n and "categories" not in n
    assert "is_set" not in n
    assert "set_interpretation" not in n
    assert "constraints" not in n and "member_ids" not in n
    e = out["edges"]["e"]
    assert e["predicates"] == ["biolink:treats_or_applied_or_studied_to_treat"]
    assert "knowledge_type" not in e
    assert "attribute_constraints" not in e and "qualifier_constraints" not in e


def test_queries_equivalent_treats_predicate_synonyms_as_same():
    a = {
        "nodes": {"x": {"ids": ["MONDO:1"]}},
        "edges": {"e": {"subject": "x", "object": "x", "predicates": ["biolink:treats"]}},
    }
    b = {
        "nodes": {"x": {"ids": ["MONDO:1"]}},
        "edges": {
            "e": {
                "subject": "x",
                "object": "x",
                "predicates": ["biolink:treats_or_applied_or_studied_to_treat"],
            }
        },
    }
    assert queries_equivalent(a, b) is True


def test_queries_equivalent_distinguishes_different_predicates():
    a = {
        "nodes": {"x": {}},
        "edges": {"e": {"subject": "x", "object": "x", "predicates": ["biolink:related_to"]}},
    }
    b = {
        "nodes": {"x": {}},
        "edges": {"e": {"subject": "x", "object": "x", "predicates": ["biolink:treats"]}},
    }
    assert queries_equivalent(a, b) is False


def test_normalize_query_does_not_mutate_input():
    """The previous implementation deep-copied; the new one must not mutate."""
    q = {
        "nodes": {"n": {"ids": None, "is_set": False, "set_interpretation": "BATCH"}},
        "edges": {},
    }
    snapshot = copy.deepcopy(q)
    _normalize_query(q)
    assert q == snapshot


def test_has_unique_nodes_false_when_two_qnodes_share_binding():
    result = {
        "node_bindings": {
            "n0": [{"id": "A"}],
            "n1": [{"id": "A"}],  # duplicate
        }
    }
    assert has_unique_nodes(result) is False


def test_has_unique_nodes_true_for_distinct_bindings():
    result = {
        "node_bindings": {
            "n0": [{"id": "A"}],
            "n1": [{"id": "B"}],
        }
    }
    assert has_unique_nodes(result) is True


def test_filter_repeated_nodes_drops_results_with_repeated_kvalues():
    response = {
        "message": {
            "query_graph": {},
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "auxiliary_graphs": {},
            "results": [
                {
                    "node_bindings": {"a": [{"id": "X"}], "b": [{"id": "X"}]},
                    "analyses": [{"edge_bindings": {}}],
                },
                {
                    "node_bindings": {"a": [{"id": "Y"}], "b": [{"id": "Z"}]},
                    "analyses": [{"edge_bindings": {}}],
                },
            ],
        }
    }
    filter_repeated_nodes(response, logger)
    remaining = response["message"]["results"]
    assert len(remaining) == 1
    assert remaining[0]["node_bindings"]["a"][0]["id"] == "Y"


def test_filter_repeated_nodes_no_results_is_a_noop():
    response = {"message": {"results": []}}
    filter_repeated_nodes(response, logger)
    assert response["message"]["results"] == []


def test_get_promiscuous_qnodes_finds_shared_subject():
    """Two edges sharing a subject with the same predicate -> the subject is the
    promiscuous (center) node candidate."""
    response = {
        "message": {
            "query_graph": {
                "nodes": {"a": {}, "b": {}, "c": {}, "d": {}},
                "edges": {
                    "e1": {"subject": "c", "object": "a", "predicates": ["biolink:p"]},
                    "e2": {"subject": "c", "object": "b", "predicates": ["biolink:p"]},
                    "e3": {"subject": "d", "object": "a", "predicates": ["biolink:p"]},
                },
            }
        }
    }
    out = get_promiscuous_qnodes(response)
    assert "c" in out


def test_get_promiscuous_qnodes_returns_empty_for_few_edges():
    response = {"message": {"query_graph": {"nodes": {}, "edges": {"e1": {}}}}}
    assert get_promiscuous_qnodes(response) == []


def test_remove_promiscuous_knode_results_drops_overrepresented_knode():
    """Construct an oversubscribed knode and verify it gets pruned."""
    response = {
        "message": {
            "results": [
                {"node_bindings": {"qx": [{"id": "BOZO"}]}}
                for _ in range(15)
            ]
            + [
                {"node_bindings": {"qx": [{"id": "GOOD"}]}},
            ],
        }
    }
    remove_promiscuous_knode_results(MAX_C=10, qnode="qx", response=response)
    remaining_ids = [r["node_bindings"]["qx"][0]["id"] for r in response["message"]["results"]]
    assert "BOZO" not in remaining_ids
    assert remaining_ids == ["GOOD"]


def test_filter_promiscuous_results_short_circuits_when_results_below_threshold():
    response = {"message": {"results": [{"node_bindings": {}, "analyses": []}]}}
    # Should be a noop; no query_graph access needed.
    filter_promiscuous_results(response, logger)
    assert len(response["message"]["results"]) == 1


def test_get_answer_node_returns_unpinned_qnode():
    qg = {"nodes": {"a": {"ids": ["X:1"]}, "b": {}}}
    assert get_answer_node(qg) == "b"


def test_get_answer_node_returns_none_when_multiple_unpinned():
    qg = {"nodes": {"a": {}, "b": {}}}
    assert get_answer_node(qg) is None


def test_group_results_by_qnode_partitions_into_creative_and_lookup():
    result_message = {
        "message": {
            "query_graph": {},
            "results": [
                {
                    "node_bindings": {
                        "qn": [{"id": "X"}],
                    },
                    "analyses": [{"edge_bindings": {}}],
                },
            ],
        }
    }
    lookup_results = [
        {
            "node_bindings": {"qn": [{"id": "X"}]},
            "analyses": [{"edge_bindings": {}}],
        },
        {
            "node_bindings": {"qn": [{"id": "Y"}]},
            "analyses": [{"edge_bindings": {}}],
        },
    ]
    grouped = group_results_by_qnode("qn", result_message, lookup_results)
    # X has both a creative result and a lookup result.
    x_key = frozenset(["X"])
    y_key = frozenset(["Y"])
    assert len(grouped[x_key]["creative"]) == 1
    assert len(grouped[x_key]["lookup"]) == 1
    assert grouped[y_key]["creative"] == []
    assert len(grouped[y_key]["lookup"]) == 1


def test_merge_messages_unsupported_query_type_raises():
    """A query graph without ``edges`` or ``paths`` should raise."""
    with pytest.raises(TypeError, match="Unsupported"):
        merge_messages(
            target="t",
            original_query_graph={"nodes": {}},
            response={"message": {"query_graph": {"nodes": {}}, "results": []}},
            new_response={"message": {"query_graph": {"nodes": {}}, "results": []}},
            logger=logger,
        )


def test_merge_messages_lookup_only_returns_new_response_directly():
    """A direct lookup with no creative answer node returns the new response as-is."""
    qg = {
        "nodes": {"a": {"ids": ["X:1"]}, "b": {"ids": ["Y:2"]}},
        "edges": {"e0": {"subject": "a", "object": "b", "predicates": ["biolink:p"]}},
    }
    new_response = {
        "message": {
            "query_graph": qg,
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "results": [
                {
                    "node_bindings": {
                        "a": [{"id": "X:1"}],
                        "b": [{"id": "Y:2"}],
                    },
                    "analyses": [{"edge_bindings": {}}],
                },
            ],
            "auxiliary_graphs": {},
        }
    }
    out = merge_messages(
        target="t",
        original_query_graph=qg,
        response={"message": {"query_graph": qg, "knowledge_graph": {"nodes": {}, "edges": {}}, "results": [], "auxiliary_graphs": {}}},
        new_response=new_response,
        logger=logger,
    )
    # No creative answer node -> direct lookup return path.
    assert len(out["message"]["results"]) == 1


def test_merge_messages_combines_aux_graphs():
    """Aux graphs from both messages should land in the merged auxiliary_graphs."""
    response = generate_response()
    callback = copy.deepcopy(response_2)
    response["message"]["auxiliary_graphs"]["aux_a"] = {
        "edges": ["a-edge"],
        "attributes": [{"foo": "bar"}],
    }
    callback["message"]["auxiliary_graphs"]["aux_b"] = {
        "edges": ["b-edge"],
        "attributes": [],
    }
    out = merge_messages(
        target="aragorn",
        original_query_graph=response_1["message"]["query_graph"],
        response=response,
        new_response=callback,
        logger=logger,
    )
    aux = out["message"]["auxiliary_graphs"]
    assert "aux_a" in aux
    assert "aux_b" in aux


def test_merge_messages_pathfinder_query_returns_single_result():
    """A pathfinder query (paths instead of edges) should return a single
    pathfinder result with the start/end node IDs threaded through."""
    pathfinder_qg = {
        "nodes": {
            "n0": {"ids": ["MONDO:0001"]},
            "n1": {"ids": ["MONDO:0002"]},
        },
        "paths": {
            "p1": {"subject": "n0", "object": "n1"},
        },
    }
    new_response = {
        "message": {
            "query_graph": pathfinder_qg,
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "auxiliary_graphs": {},
            "results": [
                {
                    "node_bindings": {
                        "n0": [{"id": "MONDO:0001"}],
                        "n1": [{"id": "MONDO:0002"}],
                    },
                    "analyses": [
                        {
                            "edge_bindings": {"p1": [{"id": "kedge_pathfinder"}]},
                        }
                    ],
                    "score": 0.42,
                }
            ],
        }
    }
    response = {
        "message": {
            "query_graph": pathfinder_qg,
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "auxiliary_graphs": {},
            "results": [],
        }
    }
    out = merge_messages(
        target="aragorn",
        original_query_graph=pathfinder_qg,
        response=response,
        new_response=new_response,
        logger=logger,
    )
    assert len(out["message"]["results"]) == 1
    pf = out["message"]["results"][0]
    assert pf["node_bindings"]["n0"][0]["id"] == "MONDO:0001"
    assert pf["node_bindings"]["n1"][0]["id"] == "MONDO:0002"
    # An auxiliary graph should have been created and associated with an
    # analysis path binding.
    assert pf["analyses"][0]["score"] == 0.42
    assert "p1" in pf["analyses"][0]["path_bindings"]


def test_merge_messages_pathfinder_missing_endpoint_raises():
    """A pathfinder query with no subject/object should raise KeyError."""
    bad_qg = {"nodes": {}, "paths": {"p1": {"subject": None, "object": "n1"}}}
    with pytest.raises(KeyError, match="subject or object"):
        merge_messages(
            target="t",
            original_query_graph=bad_qg,
            response={
                "message": {
                    "query_graph": bad_qg,
                    "knowledge_graph": {"nodes": {}, "edges": {}},
                    "auxiliary_graphs": {},
                    "results": [],
                }
            },
            new_response={
                "message": {
                    "query_graph": bad_qg,
                    "knowledge_graph": {"nodes": {}, "edges": {}},
                    "auxiliary_graphs": {},
                    "results": [],
                }
            },
            logger=logger,
        )


def test_merge_messages_creative_query_creates_knowledge_edges():
    """A creative-mode query (matches existing test fixture) should have its
    inferred creative results converted into synthetic knowledge edges."""
    original_qg = generate_query()["message"]["query_graph"]
    response = generate_response()
    callback = copy.deepcopy(response_2)

    out = merge_messages(
        target="aragorn",
        original_query_graph=original_qg,
        response=response,
        new_response=callback,
        logger=logger,
    )
    edges = out["message"]["knowledge_graph"]["edges"]
    # Every creative result should have produced at least one synthetic edge
    # whose source is the shepherd-aragorn primary kg.
    shepherd_edges = [
        e
        for e in edges.values()
        if any(
            s.get("resource_id") == "infores:shepherd-aragorn"
            and s.get("resource_role") == "primary_knowledge_source"
            for s in e.get("sources", [])
        )
    ]
    assert shepherd_edges
