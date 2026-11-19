"""Consecutive lightweight workflow steps run in one load/save pass (#219)."""

import json
import logging

from shepherd_utils.db import get_message, save_message
from workers.sort_results_score.worker import sort_results_score

logger = logging.getLogger(__name__)

DEFAULT_TAIL = [
    {"id": "sort_results_score"},
    {"id": "filter_results_top_n", "parameters": {"max_results": 1}},
    {"id": "filter_kgraph_orphans"},
]


def _edge(subject, obj):
    return {
        "subject": subject,
        "object": obj,
        "predicate": "biolink:related_to",
        "knowledge_level": "knowledge_assertion",
        "agent_type": "manual_agent",
        "sources": [
            {"resource_id": "infores:kp", "resource_role": "primary_knowledge_source"}
        ],
    }


def _response():
    """Two answers; the lower-scored one's node and edge are used only by it."""
    nodes = {c: {"categories": ["biolink:NamedThing"]} for c in ("A", "B", "C")}
    return {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": ["A"]}, "n1": {}},
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            },
            "knowledge_graph": {
                "nodes": nodes,
                "edges": {"ab": _edge("A", "B"), "ac": _edge("A", "C")},
            },
            "results": [
                {
                    "node_bindings": {"n0": {"ids": ["A"]}, "n1": {"ids": ["C"]}},
                    "analyses": [
                        {
                            "resource_id": "infores:x",
                            "edge_bindings": {"e0": {"ids": ["ac"]}},
                            "score": 0.2,
                        }
                    ],
                },
                {
                    "node_bindings": {"n0": {"ids": ["A"]}, "n1": {"ids": ["B"]}},
                    "analyses": [
                        {
                            "resource_id": "infores:x",
                            "edge_bindings": {"e0": {"ids": ["ab"]}},
                            "score": 0.9,
                        }
                    ],
                },
            ],
        }
    }


def _task(workflow):
    return [
        "1-0",
        {
            "query_id": "q",
            "response_id": "r",
            "workflow": json.dumps(workflow),
            "log_level": "20",
            "otel": "{}",
        },
    ]


async def test_the_default_tail_runs_in_one_pass(redis_mock, mocker):
    """sort -> filter -> orphans used to be three worker hops, each loading
    and saving the whole response. The sort worker now does all three."""
    await save_message("r", _response(), logger)
    saves = mocker.spy(
        __import__("workers.sort_results_score.worker", fromlist=["x"]),
        "save_response",
    )
    task = _task(DEFAULT_TAIL)

    await sort_results_score(task, logger)

    assert saves.call_count == 1
    message = await get_message("r", logger)
    (result,) = message["message"]["results"]
    assert result["node_bindings"]["n1"]["ids"] == ["B"]
    assert set(message["message"]["knowledge_graph"]["edges"]) == {"ab"}
    assert set(message["message"]["knowledge_graph"]["nodes"]) == {"A", "B"}
    # wrap_up_task drops the head and hands on to what follows: nothing here,
    # so finish_query -- not filter_results_top_n.
    assert json.loads(task[1]["workflow"]) == [{"id": "sort_results_score"}]


async def test_the_run_stops_at_a_step_that_is_not_local(redis_mock):
    await save_message("r", _response(), logger)
    workflow = [
        DEFAULT_TAIL[0],
        DEFAULT_TAIL[1],
        {"id": "aragorn.score"},
        DEFAULT_TAIL[2],
    ]
    task = _task(workflow)

    await sort_results_score(task, logger)

    assert json.loads(task[1]["workflow"]) == [
        DEFAULT_TAIL[0],
        {"id": "aragorn.score"},
        DEFAULT_TAIL[2],
    ]
    # filtered, but the orphan removal waits for its own turn
    message = await get_message("r", logger)
    assert len(message["message"]["results"]) == 1


async def test_truncation_drops_the_dropped_results_graph(redis_mock):
    """C5-7: filter_results_top_n left the dropped results' nodes and edges
    in the knowledge graph."""
    from workers.filter_results_top_n.worker import filter_results_top_n

    response = _response()
    response["message"]["results"].reverse()  # best first
    await save_message("r", response, logger)

    await filter_results_top_n(_task([DEFAULT_TAIL[1]]), logger)

    message = await get_message("r", logger)
    assert set(message["message"]["knowledge_graph"]["edges"]) == {"ab"}


def _scored(score):
    analysis = {"resource_id": "infores:x", "edge_bindings": {"e0": {"ids": ["ab"]}}}
    if score is not None:
        analysis["score"] = score
    return {"analyses": [analysis], "tag": score}


async def test_unscored_results_sort_last_in_either_direction(redis_mock):
    """C5-8: an unscored result sorted as 0, so with negative scores it ranked
    above every scored one."""
    for direction, expected in (
        ("descending", [-1, -2, None]),
        ("ascending", [-2, -1, None]),
    ):
        await save_message(
            "r",
            {"message": {"results": [_scored(None), _scored(-2), _scored(-1)]}},
            logger,
        )
        await sort_results_score(
            _task([{"id": "sort_results_score", "ascending_or_descending": direction}]),
            logger,
        )
        message = await get_message("r", logger)
        assert [r["tag"] for r in message["message"]["results"]] == expected
