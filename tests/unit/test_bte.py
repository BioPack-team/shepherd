"""Tests for ``workers.bte.worker``: ``examine_query`` and the entry-point
workflow construction."""

import copy
import json
import logging

import pytest

from tests.helpers.generate_messages import creative_query
from workers.bte.worker import bte, examine_query

logger = logging.getLogger(__name__)


def test_examine_query_pure_lookup_returns_no_question_or_answer():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        }
    }
    infer, q, a, pathfinder = examine_query(msg)
    assert (infer, pathfinder) == (False, False)
    assert q is None and a is None


def test_examine_query_inferred_returns_question_and_answer():
    msg = copy.deepcopy(creative_query)
    infer, q, a, pathfinder = examine_query(msg)
    assert infer is True
    assert (q, a) == ("ON", "SN")


def test_examine_query_pathfinder_three_inferred_edges():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {"ids": ["Y:1"]}},
                "paths": {
                    "e0": {"subject": "a", "object": "b"},
                },
            }
        }
    }
    _, _, _, pathfinder = examine_query(msg)
    assert pathfinder is True


def test_examine_query_rejects_two_inferred_when_not_pathfinder():
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
                "nodes": {"a": {"ids": ["X:1"]}, "b": {"ids": ["Y:2"]}},
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
                "nodes": {"a": {}, "b": {}},
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


def _make_task(workflow=None):
    return [
        "test",
        {
            "query_id": "qid",
            "response_id": "rid",
            "workflow": json.dumps(workflow),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]


@pytest.mark.asyncio
async def test_bte_lookup_workflow(redis_mock, mocker):
    """Pure-lookup query: install the standard BTE workflow."""
    mocker.patch(
        "workers.bte.worker.get_message",
        new_callable=mocker.AsyncMock,
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
    await bte(task, logger)
    workflow = json.loads(task[1]["workflow"])
    assert [op["id"] for op in workflow] == [
        "bte.lookup",
        "aragorn.omnicorp",
        "aragorn.score",
        "sort_results_score",
        "filter_results_top_n",
        "filter_kgraph_orphans",
    ]


@pytest.mark.asyncio
async def test_bte_inferred_workflow_matches_lookup(redis_mock, mocker):
    """Inferred (creative) workflow currently mirrors the lookup workflow."""
    mocker.patch(
        "workers.bte.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=copy.deepcopy(creative_query),
    )
    task = _make_task(None)
    await bte(task, logger)
    workflow = json.loads(task[1]["workflow"])
    assert [op["id"] for op in workflow][0] == "bte.lookup"


@pytest.mark.asyncio
async def test_bte_rejects_pathfinder_query(redis_mock, mocker):
    """BTE explicitly rejects pathfinder queries."""
    pathfinder_msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {"ids": ["Y:1"]}},
                "paths": {
                    "e0": {"subject": "a", "object": "b"},
                },
            }
        }
    }
    mocker.patch(
        "workers.bte.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=pathfinder_msg,
    )
    with pytest.raises(Exception, match="does not support Pathfinder"):
        await bte(_make_task(None), logger)


@pytest.mark.asyncio
async def test_bte_preexisting_workflow_is_preserved(redis_mock, mocker):
    """If the task already has a workflow, bte() leaves it alone."""
    mocker.patch(
        "workers.bte.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=copy.deepcopy(creative_query),
    )
    custom = [{"id": "bte.lookup"}]
    task = _make_task(custom)
    await bte(task, logger)
    assert json.loads(task[1]["workflow"]) == custom
