"""Tests for ``workers.aragorn.worker.examine_query`` and the workflow
selection logic on the entrypoint.

These exercise the pure-logic ``examine_query`` against the same query
shapes the production code routes (lookup, infer, pathfinder, mixed) and
verify that the auto-generated workflow matches the query type.
"""

import copy
import json
import logging

import pytest

from tests.helpers.generate_messages import creative_query
from workers.aragorn.worker import aragorn, examine_query

logger = logging.getLogger(__name__)


def _make_task(message_lookup, workflow=None):
    """Build the (msg_id, fields) tuple a worker receives."""
    return [
        "test",
        {
            "query_id": "test",
            "response_id": "test_response",
            "workflow": json.dumps(workflow),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]


def test_examine_query_pure_lookup_returns_no_question_or_answer():
    """All-lookup queries: infer=False, pathfinder=False, no nodes returned."""
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["MONDO:1"]}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        }
    }
    infer, q, a, pathfinder = examine_query(msg)
    assert (infer, pathfinder) == (False, False)
    assert q is None and a is None


def test_examine_query_inferred_one_hop_returns_question_and_answer():
    msg = copy.deepcopy(creative_query)
    infer, q, a, pathfinder = examine_query(msg)
    assert infer is True
    assert pathfinder is False
    # The fixture pins ON, leaves SN unbound -> answer node SN
    assert q == "ON" and a == "SN"


def test_examine_query_pathfinder_returns_pathfinder_true():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {},
                "edges": {},
                "paths": {"p1": {"subject": "n0", "object": "n1"}},
            }
        }
    }
    infer, q, a, pathfinder = examine_query(msg)
    assert pathfinder is True
    # No edges, so not infer
    assert infer is False


def test_examine_query_rejects_multiple_paths():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {},
                "edges": {},
                "paths": {"p1": {}, "p2": {}},
            }
        }
    }
    with pytest.raises(Exception, match="single path"):
        examine_query(msg)


def test_examine_query_rejects_mixed_path_and_edges():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {},
                "edges": {"e0": {"subject": "a", "object": "b"}},
                "paths": {"p1": {}},
            }
        }
    }
    with pytest.raises(Exception, match="Mixed mode pathfinder"):
        examine_query(msg)


def test_examine_query_rejects_multiple_inferred_edges():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {
                    "e0": {"subject": "a", "object": "b", "knowledge_type": "inferred"},
                    "e1": {"subject": "a", "object": "b", "knowledge_type": "inferred"},
                },
            }
        }
    }
    with pytest.raises(Exception, match="single infer edge"):
        examine_query(msg)


def test_examine_query_rejects_mixed_lookup_and_infer():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {
                    "e0": {"subject": "a", "object": "b", "knowledge_type": "inferred"},
                    "e1": {"subject": "a", "object": "b"},
                },
            }
        }
    }
    with pytest.raises(Exception, match="Mixed infer and lookup"):
        examine_query(msg)


def test_examine_query_rejects_both_creative_nodes_pinned():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {
                    "a": {"ids": ["X:1"]},
                    "b": {"ids": ["Y:2"]},
                },
                "edges": {
                    "e0": {
                        "subject": "a",
                        "object": "b",
                        "knowledge_type": "inferred",
                    },
                },
            }
        }
    }
    with pytest.raises(Exception, match="Both nodes of creative edge pinned"):
        examine_query(msg)


def test_examine_query_rejects_no_creative_node_pinned():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {
                    "a": {},
                    "b": {},
                },
                "edges": {
                    "e0": {
                        "subject": "a",
                        "object": "b",
                        "knowledge_type": "inferred",
                    },
                },
            }
        }
    }
    with pytest.raises(Exception, match="No nodes of creative edge pinned"):
        examine_query(msg)


@pytest.mark.asyncio
async def test_aragorn_pathfinder_workflow(redis_mock, mocker):
    """A pathfinder query routes through aragorn.pathfinder and gandalf.rehydrate."""
    mocker.patch(
        "workers.aragorn.worker.get_message",
        return_value={
            "message": {
                "query_graph": {
                    "nodes": {},
                    "edges": {},
                    "paths": {"p1": {"subject": "a", "object": "b"}},
                }
            }
        },
    )
    task = _make_task(None)
    await aragorn(task, logger)
    workflow = json.loads(task[1]["workflow"])
    assert [op["id"] for op in workflow] == [
        "aragorn.pathfinder",
        "score_paths",
        "sort_results_score",
        "filter_analyses_top_n",
        "filter_kgraph_orphans",
        "gandalf.rehydrate",
    ]


@pytest.mark.asyncio
async def test_aragorn_lookup_only_workflow(redis_mock, mocker):
    """A pure-lookup query (no inferred edges) gets the lookup workflow."""
    mocker.patch(
        "workers.aragorn.worker.get_message",
        return_value={
            "message": {
                "query_graph": {
                    "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                    "edges": {"e0": {"subject": "a", "object": "b"}},
                }
            }
        },
    )
    task = _make_task(None)
    await aragorn(task, logger)
    workflow = json.loads(task[1]["workflow"])
    assert [op["id"] for op in workflow] == [
        "aragorn.lookup",
        "aragorn.omnicorp",
        "aragorn.score",
        "sort_results_score",
        "filter_results_top_n",
        "filter_kgraph_orphans",
    ]


@pytest.mark.asyncio
async def test_aragorn_preexisting_workflow_is_preserved(redis_mock, mocker):
    """If a workflow is already on the task, aragorn() should not overwrite it."""
    mocker.patch(
        "workers.aragorn.worker.get_message",
        return_value=copy.deepcopy(creative_query),
    )
    custom_workflow = [{"id": "aragorn.lookup"}]
    task = _make_task(custom_workflow, workflow=custom_workflow)
    await aragorn(task, logger)
    assert json.loads(task[1]["workflow"]) == custom_workflow
