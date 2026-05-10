"""Tests for ``shepherd_utils.shared`` helper functions.

These exercise the pure helpers (``combine_unique_dicts``, ``merge_kgraph``,
``is_support_edge``, the recursive support-graph traversal, and
``filter_kgraph_orphans``) plus the broker-driven workflow helpers
(``get_next_operation``, ``wrap_up_task``, ``handle_task_failure``).
"""

import json
import logging

import pytest

from shepherd_utils.broker import get_task
from shepherd_utils.shared import (
    combine_unique_dicts,
    filter_kgraph_orphans,
    get_next_operation,
    handle_task_failure,
    is_support_edge,
    merge_kgraph,
    recursive_get_auxgraph_edges,
    recursive_get_edge_support_graphs,
    validate_message,
    wrap_up_task,
)


logger = logging.getLogger(__name__)


def test_get_next_operation_returns_first_op():
    workflow = [{"id": "first"}, {"id": "second"}]
    next_op, returned = get_next_operation(workflow)
    assert next_op == {"id": "first"}
    # Function returns the same workflow back; it does not mutate it.
    assert returned is workflow
    assert returned == [{"id": "first"}, {"id": "second"}]


def test_combine_unique_dicts_dedupes_across_lists():
    a = [{"x": 1}, {"y": 2}]
    b = [{"x": 1}, {"z": 3}]
    out = combine_unique_dicts(a, b, logger)
    assert {"x": 1} in out
    assert {"y": 2} in out
    assert {"z": 3} in out
    assert len(out) == 3


def test_combine_unique_dicts_treats_key_order_as_equal():
    a = [{"a": 1, "b": 2}]
    b = [{"b": 2, "a": 1}]
    out = combine_unique_dicts(a, b, logger)
    assert out == [{"a": 1, "b": 2}]


def test_combine_unique_dicts_handles_unhashable_values_via_default_str():
    """Items with non-JSON values (e.g. sets) shouldn't blow up: default=str
    makes the signature stable."""

    class NotSerializable:
        def __str__(self):
            return "constant-token"

    a = [{"v": NotSerializable()}]
    b = [{"v": NotSerializable()}]
    out = combine_unique_dicts(a, b, logger)
    assert len(out) == 1


def test_is_support_edge_true_for_support_graph_attribute():
    edge = {
        "attributes": [
            {"attribute_type_id": "biolink:support_graphs", "value": ["aux1"]},
        ],
    }
    assert is_support_edge(edge) is True


def test_is_support_edge_false_for_no_attributes():
    assert is_support_edge({}) is False


def test_is_support_edge_false_for_non_support_attributes():
    edge = {
        "attributes": [
            {"attribute_type_id": "biolink:has_evidence", "value": 5},
        ],
    }
    assert is_support_edge(edge) is False


def test_merge_kgraph_adds_new_nodes_and_appends_aggregator_source():
    og = {"nodes": {}, "edges": {}}
    new = {
        "nodes": {
            "MONDO:1": {"name": "n1", "categories": ["biolink:Disease"], "attributes": []},
        },
        "edges": {
            "e1": {
                "subject": "MONDO:1",
                "object": "MONDO:2",
                "attributes": [],
                "sources": [
                    {
                        "resource_id": "infores:original",
                        "resource_role": "primary_knowledge_source",
                    }
                ],
            },
        },
    }
    merged = merge_kgraph(og, new, "infores:test", logger)
    assert "MONDO:1" in merged["nodes"]
    assert "e1" in merged["edges"]
    sources = merged["edges"]["e1"]["sources"]
    aggregator_ids = [s["resource_id"] for s in sources]
    assert "infores:test" in aggregator_ids


def test_merge_kgraph_merges_existing_node_attributes_and_categories():
    og = {
        "nodes": {
            "MONDO:1": {
                "name": "",
                "categories": ["biolink:Disease"],
                "attributes": [{"attribute_type_id": "biolink:foo", "value": 1}],
            },
        },
        "edges": {},
    }
    new = {
        "nodes": {
            "MONDO:1": {
                "name": "Updated",
                "categories": ["biolink:DiseaseOrPhenotypicFeature"],
                "attributes": [{"attribute_type_id": "biolink:bar", "value": 2}],
            },
        },
        "edges": {},
    }
    merged = merge_kgraph(og, new, "infores:test", logger)
    node = merged["nodes"]["MONDO:1"]
    assert node["name"] == "Updated"
    assert set(node["categories"]) == {
        "biolink:Disease",
        "biolink:DiseaseOrPhenotypicFeature",
    }
    # Both attributes preserved through dedupe.
    type_ids = {a["attribute_type_id"] for a in node["attributes"]}
    assert {"biolink:foo", "biolink:bar"}.issubset(type_ids)


def test_merge_kgraph_does_not_double_aggregator_source():
    aggregator = {
        "resource_id": "infores:test",
        "resource_role": "aggregator_knowledge_source",
        "upstream_resource_ids": ["infores:retriever"],
    }
    og = {"nodes": {}, "edges": {}}
    new = {
        "nodes": {},
        "edges": {
            "e1": {
                "subject": "A",
                "object": "B",
                "attributes": [],
                "sources": [aggregator],
            },
        },
    }
    merged = merge_kgraph(og, new, "infores:test", logger)
    sources = merged["edges"]["e1"]["sources"]
    matching = [s for s in sources if s["resource_id"] == "infores:test"]
    assert len(matching) == 1


def test_merge_kgraph_does_not_append_aggregator_to_support_edge():
    """Support edges (those carrying biolink:support_graphs attributes) should
    not have a new aggregator source appended."""
    og = {"nodes": {}, "edges": {}}
    new = {
        "nodes": {},
        "edges": {
            "e1": {
                "subject": "A",
                "object": "B",
                "attributes": [
                    {"attribute_type_id": "biolink:support_graphs", "value": ["aux1"]},
                ],
                "sources": [
                    {
                        "resource_id": "infores:original",
                        "resource_role": "primary_knowledge_source",
                    }
                ],
            },
        },
    }
    merged = merge_kgraph(og, new, "infores:test", logger)
    sources = merged["edges"]["e1"]["sources"]
    assert all(s["resource_id"] != "infores:test" for s in sources)


def test_recursive_get_edge_support_graphs_short_circuits_on_visited_edge():
    """Edges already in the visited set should not be re-traversed (otherwise
    circular support graphs would loop forever)."""
    edges = {"already-seen"}
    auxgraphs = set()
    nodes = set()
    out = recursive_get_edge_support_graphs(
        "already-seen",
        edges,
        auxgraphs,
        message_edges={"already-seen": {"subject": "A", "object": "B"}},
        message_auxgraphs={},
        nodes=nodes,
    )
    assert out == (edges, auxgraphs, nodes)


def test_recursive_get_edge_support_graphs_walks_into_auxgraphs():
    message_edges = {
        "edge1": {
            "subject": "A",
            "object": "B",
            "attributes": [
                {"attribute_type_id": "biolink:support_graphs", "value": ["aux1"]},
            ],
        },
        "edge2": {
            "subject": "B",
            "object": "C",
            "attributes": [],
        },
    }
    message_auxgraphs = {"aux1": {"edges": ["edge2"]}}
    edges, auxgraphs, nodes = recursive_get_edge_support_graphs(
        "edge1", set(), set(), message_edges, message_auxgraphs, set()
    )
    assert {"edge1", "edge2"} == edges
    assert {"aux1"} == auxgraphs
    assert {"A", "B", "C"} == nodes


def test_recursive_get_auxgraph_edges_raises_for_missing_aux_edge():
    message_auxgraphs = {"aux1": {"edges": ["missing"]}}
    with pytest.raises(KeyError, match="missing"):
        recursive_get_auxgraph_edges(
            "aux1", set(), set(), message_edges={}, message_auxgraphs=message_auxgraphs, nodes=set()
        )


def test_recursive_get_edge_support_graphs_raises_for_unknown_auxgraph():
    message_edges = {
        "edge1": {
            "subject": "A",
            "object": "B",
            "attributes": [
                {"attribute_type_id": "biolink:support_graphs", "value": ["missing"]},
            ],
        }
    }
    with pytest.raises(KeyError, match="missing"):
        recursive_get_edge_support_graphs(
            "edge1", set(), set(), message_edges, message_auxgraphs={}, nodes=set()
        )


def test_validate_message_keeps_valid_message(tmp_path, monkeypatch):
    """A message whose edges all reference existing nodes should not write the
    invalid_message.json side effect file."""
    monkeypatch.chdir(tmp_path)
    message = {
        "message": {
            "knowledge_graph": {
                "nodes": {"A": {}, "B": {}},
                "edges": {"e1": {"subject": "A", "object": "B"}},
            },
            "auxiliary_graphs": {},
        },
    }
    validate_message(message, logger)
    assert not (tmp_path / "invalid_message.json").exists()


def test_validate_message_dumps_invalid_message_on_missing_node(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    message = {
        "message": {
            "knowledge_graph": {
                "nodes": {"A": {}},
                "edges": {"e1": {"subject": "A", "object": "MISSING"}},
            },
            "auxiliary_graphs": {},
        },
    }
    validate_message(message, logger)
    out = tmp_path / "invalid_message.json"
    assert out.exists()
    with open(out, encoding="utf-8") as f:
        dumped = json.load(f)
    assert dumped == message


def test_filter_kgraph_orphans_keeps_support_graph_chain():
    """An edge that references an aux graph that references another edge: all
    of that should be retained."""
    message = {
        "message": {
            "knowledge_graph": {
                "nodes": {"A": {}, "B": {}, "C": {}, "ORPHAN": {}},
                "edges": {
                    "result_edge": {
                        "subject": "A",
                        "object": "B",
                        "attributes": [
                            {
                                "attribute_type_id": "biolink:support_graphs",
                                "value": ["aux1"],
                            }
                        ],
                    },
                    "support_edge": {
                        "subject": "B",
                        "object": "C",
                        "attributes": [],
                    },
                    "orphan_edge": {
                        "subject": "ORPHAN",
                        "object": "A",
                        "attributes": [],
                    },
                },
            },
            "auxiliary_graphs": {
                "aux1": {"edges": ["support_edge"]},
                "aux_orphan": {"edges": ["orphan_edge"]},
            },
            "results": [
                {
                    "node_bindings": {
                        "qn1": [{"id": "A"}],
                        "qn2": [{"id": "B"}],
                    },
                    "analyses": [
                        {"edge_bindings": {"e0": [{"id": "result_edge"}]}},
                    ],
                }
            ],
        },
    }
    filter_kgraph_orphans(message, logger)
    nodes = message["message"]["knowledge_graph"]["nodes"]
    edges = message["message"]["knowledge_graph"]["edges"]
    auxgraphs = message["message"]["auxiliary_graphs"]
    assert set(nodes.keys()) == {"A", "B", "C"}
    assert set(edges.keys()) == {"result_edge", "support_edge"}
    assert set(auxgraphs.keys()) == {"aux1"}


def test_filter_kgraph_orphans_warns_on_missing_aux_edge_and_continues():
    """If a support graph references an edge that's not in the kg, we drop it
    but don't crash the whole filter."""
    message = {
        "message": {
            "knowledge_graph": {
                "nodes": {"A": {}, "B": {}},
                "edges": {
                    "result_edge": {
                        "subject": "A",
                        "object": "B",
                        "attributes": [
                            {
                                "attribute_type_id": "biolink:support_graphs",
                                "value": ["bad_aux"],
                            }
                        ],
                    },
                },
            },
            "auxiliary_graphs": {
                "bad_aux": {"edges": ["nonexistent_edge"]},
            },
            "results": [
                {
                    "node_bindings": {"qn1": [{"id": "A"}], "qn2": [{"id": "B"}]},
                    "analyses": [
                        {"edge_bindings": {"e0": [{"id": "result_edge"}]}},
                    ],
                }
            ],
        },
    }
    filter_kgraph_orphans(message, logger)
    # result_edge is still kept because it was directly bound, even if its
    # support graph couldn't be fully resolved.
    assert "result_edge" in message["message"]["knowledge_graph"]["edges"]


def test_filter_kgraph_orphans_handles_path_bindings_and_support_graphs():
    """Pathfinder-style results: path_bindings + support_graphs analyses both
    pull aux graphs in."""
    message = {
        "message": {
            "knowledge_graph": {
                "nodes": {"A": {}, "B": {}, "C": {}},
                "edges": {
                    "edge_via_path": {"subject": "A", "object": "B", "attributes": []},
                    "edge_via_support": {"subject": "B", "object": "C", "attributes": []},
                },
            },
            "auxiliary_graphs": {
                "aux_path": {"edges": ["edge_via_path"]},
                "aux_support": {"edges": ["edge_via_support"]},
            },
            "results": [
                {
                    "node_bindings": {"qn1": [{"id": "A"}]},
                    "analyses": [
                        {
                            "edge_bindings": {},
                            "path_bindings": {
                                "p0": [{"id": "aux_path"}],
                            },
                            "support_graphs": ["aux_support"],
                        },
                    ],
                }
            ],
        },
    }
    filter_kgraph_orphans(message, logger)
    auxgraphs = message["message"]["auxiliary_graphs"]
    assert {"aux_path", "aux_support"}.issubset(auxgraphs.keys())
    edges = message["message"]["knowledge_graph"]["edges"]
    assert {"edge_via_path", "edge_via_support"}.issubset(edges.keys())


def test_filter_kgraph_orphans_creates_empty_kg_when_results_present_but_kg_missing():
    """If results reference an edge but no knowledge_graph exists yet, the
    filter should still make the message structurally valid (empty kg)."""
    message = {
        "message": {
            "results": [
                {
                    "node_bindings": {"qn": [{"id": "A"}]},
                    "analyses": [{"edge_bindings": {"e0": [{"id": "missing"}]}}],
                }
            ],
        },
    }
    filter_kgraph_orphans(message, logger)
    assert message["message"]["knowledge_graph"] == {"nodes": {}, "edges": {}}
    assert message["message"]["auxiliary_graphs"] == {}


@pytest.mark.asyncio
async def test_wrap_up_task_pops_completed_op_and_queues_next(redis_mock):
    """If the worker stream matches the head of the workflow, that op is
    popped and the next one is enqueued for processing."""
    task = [
        "msg-1",
        {
            "query_id": "q1",
            "response_id": "r1",
            "workflow": json.dumps([
                {"id": "stream_a"},
                {"id": "stream_b"},
            ]),
            "log_level": "20",
            "otel": "{}",
        },
    ]
    await wrap_up_task("stream_a", "consumer", task, logger)
    next_task = await get_task("stream_b", "consumer", "test", logger)
    assert next_task is not None
    workflow = json.loads(next_task[1]["workflow"])
    assert [op["id"] for op in workflow] == ["stream_b"]


@pytest.mark.asyncio
async def test_wrap_up_task_routes_empty_workflow_to_finish_query(redis_mock):
    task = [
        "msg-2",
        {
            "query_id": "q1",
            "response_id": "r1",
            "workflow": json.dumps([{"id": "stream_a"}]),
            "log_level": "20",
            "otel": "{}",
        },
    ]
    await wrap_up_task("stream_a", "consumer", task, logger)
    next_task = await get_task("finish_query", "consumer", "test", logger)
    assert next_task is not None
    workflow = json.loads(next_task[1]["workflow"])
    assert workflow == []


@pytest.mark.asyncio
async def test_wrap_up_task_does_not_pop_for_entry_worker(redis_mock):
    """Entry workers (whose stream name doesn't match any workflow op id)
    should run the first op in the workflow, not skip it."""
    task = [
        "msg-3",
        {
            "query_id": "q1",
            "response_id": "r1",
            "workflow": json.dumps([
                {"id": "real_op"},
                {"id": "next_op"},
            ]),
            "log_level": "20",
            "otel": "{}",
        },
    ]
    await wrap_up_task("entry_stream", "consumer", task, logger)
    next_task = await get_task("real_op", "consumer", "test", logger)
    assert next_task is not None
    workflow = json.loads(next_task[1]["workflow"])
    # real_op was the first item and must still be present (not consumed).
    assert [op["id"] for op in workflow] == ["real_op", "next_op"]


@pytest.mark.asyncio
async def test_handle_task_failure_routes_to_finish_query_with_error_status(redis_mock):
    task = [
        "msg-4",
        {
            "query_id": "q1",
            "response_id": "r1",
            "workflow": json.dumps([{"id": "broken_op"}]),
            "log_level": "20",
            "otel": "{}",
        },
    ]
    await handle_task_failure("broken_op", "consumer", task, logger)
    next_task = await get_task("finish_query", "consumer", "test", logger)
    assert next_task is not None
    assert next_task[1]["status"] == "ERROR"
