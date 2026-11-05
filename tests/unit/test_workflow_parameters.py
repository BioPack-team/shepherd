"""Workflow operation settings are read from ``parameters`` and checked at
intake (#208).

TRAPI workflow operations carry their settings under ``parameters`` -- which
is how Shepherd's own default workflows write them -- but the filter and sort
workers read them from the top level of the operation, so a client's
``max_results`` / ``max_analyses`` / ``ascending_or_descending`` was ignored.
A malformed or unsupported workflow reached a worker or raised a 500 instead
of a 400, and a lookup took a negative ``parameters.timeout`` (TRAPI's "drop
the server's default") as a wait of less than nothing.
"""

import json
import logging
from unittest.mock import AsyncMock

import pytest

logger = logging.getLogger(__name__)


def _results(scores):
    return {
        "message": {
            "results": [
                {"analyses": [{"score": s} for s in analysis_scores]}
                for analysis_scores in scores
            ]
        }
    }


def _task(op):
    return ("1-0", {"response_id": "r1", "workflow": json.dumps([op])})


@pytest.fixture
def saved(monkeypatch):
    out = {}

    async def _save(response_id, message, _logger, **kwargs):
        out["message"] = message

    return out, _save


async def test_filter_results_reads_max_results_from_parameters(monkeypatch, saved):
    from workers.filter_results_top_n import worker

    out, save = saved
    monkeypatch.setattr(
        worker, "get_message", AsyncMock(return_value=_results([[3], [2], [1]]))
    )
    monkeypatch.setattr(worker, "save_response", save)

    await worker.filter_results_top_n(
        _task({"id": "filter_results_top_n", "parameters": {"max_results": 1}}),
        logger,
    )
    assert len(out["message"]["message"]["results"]) == 1


async def test_filter_results_still_reads_a_top_level_setting(monkeypatch, saved):
    from workers.filter_results_top_n import worker

    out, save = saved
    monkeypatch.setattr(
        worker, "get_message", AsyncMock(return_value=_results([[3], [2], [1]]))
    )
    monkeypatch.setattr(worker, "save_response", save)

    await worker.filter_results_top_n(
        _task({"id": "filter_results_top_n", "max_results": 2}), logger
    )
    assert len(out["message"]["message"]["results"]) == 2


async def test_filter_analyses_reads_max_analyses_from_parameters(monkeypatch, saved):
    from workers.filter_analyses_top_n import worker

    out, save = saved
    monkeypatch.setattr(
        worker, "get_message", AsyncMock(return_value=_results([[3, 2, 1]]))
    )
    monkeypatch.setattr(worker, "save_response", save)

    await worker.filter_analyses_top_n(
        _task({"id": "filter_analyses_top_n", "parameters": {"max_analyses": 1}}),
        logger,
    )
    assert len(out["message"]["message"]["results"][0]["analyses"]) == 1


async def test_sort_reads_its_direction_from_parameters(monkeypatch, saved):
    from workers.sort_results_score import worker

    out, save = saved
    monkeypatch.setattr(
        worker, "get_message", AsyncMock(return_value=_results([[1], [3], [2]]))
    )
    monkeypatch.setattr(worker, "save_response", save)

    await worker.sort_results_score(
        _task(
            {
                "id": "sort_results_score",
                "parameters": {"ascending_or_descending": "ascending"},
            }
        ),
        logger,
    )
    scores = [r["analyses"][0]["score"] for r in out["message"]["message"]["results"]]
    assert scores == [1, 2, 3]


# --- intake --------------------------------------------------------------------


@pytest.fixture
def intake(monkeypatch):
    from shepherd_server import base_routes

    monkeypatch.setattr(base_routes, "add_query", AsyncMock())
    monkeypatch.setattr(base_routes, "add_task", AsyncMock())
    return base_routes


@pytest.mark.parametrize(
    "workflow",
    [
        "not a list",
        ["not an object"],
        [{"id": "no_such_operation"}],
        [{"id": "filter_results_top_n", "parameters": {"max_results": -5}}],
        [{"id": "filter_results_top_n", "parameters": {"max_results": "ten"}}],
        [{"id": "filter_results_top_n", "parameters": {"max_results": True}}],
        [{"id": "filter_analyses_top_n", "parameters": {"max_analyses": 0}}],
        [{"id": "filter_results_top_n", "max_results": -1}],
        [
            {
                "id": "sort_results_score",
                "parameters": {"ascending_or_descending": "sideways"},
            }
        ],
        [{"id": "sort_results_score", "parameters": "not an object"}],
    ],
)
async def test_intake_rejects_a_bad_workflow_as_a_client_error(intake, workflow):
    query = {"message": {"query_graph": {"nodes": {"n0": {}}}}, "workflow": workflow}
    with pytest.raises(intake.TRAPIRequestError):
        await intake.run_query("aragorn", query, "http://callback/1")


async def test_intake_accepts_a_good_workflow(intake):
    query = {
        "message": {"query_graph": {"nodes": {"n0": {}}}},
        "workflow": [
            {"id": "aragorn.lookup"},
            {
                "id": "sort_results_score",
                "parameters": {"ascending_or_descending": "ascending"},
            },
            {"id": "filter_results_top_n", "parameters": {"max_results": 10}},
            {"id": "filter_analyses_top_n", "max_analyses": 3},
        ],
    }
    await intake.run_query("aragorn", query, "http://callback/1")


async def test_a_bad_workflow_is_a_400_on_both_routes(intake):
    from starlette.requests import Request

    body = json.dumps(
        {
            "message": {"query_graph": {"nodes": {"n0": {}}}},
            "workflow": [{"id": "no_such_operation"}],
            "callback": "http://callback/1",
        }
    ).encode()

    def request():
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.disconnect"}
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}

        scope = {
            "type": "http",
            "method": "POST",
            "path": "/aragorn/query",
            "headers": [(b"content-type", b"application/json")],
        }
        return Request(scope, receive)

    sync = await intake.run_sync_query(intake.ARATargetEnum.ARAGORN, request())
    async_ = await intake.run_async_query(intake.ARATargetEnum.ARAGORN, request())
    assert sync.status_code == 400
    assert async_.status_code == 400


# --- lookup wait -----------------------------------------------------------------


async def test_a_negative_timeout_does_not_cut_the_lookup_wait_short(monkeypatch):
    """TRAPI's negative timeout means "drop the server's default", not "wait
    for nothing": the lookup must still wait for its callbacks."""
    from workers.aragorn_lookup import worker

    query = {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": ["X:1"]}, "n1": {}},
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            }
        },
        "parameters": {"timeout": -1},
    }
    monkeypatch.setattr(worker, "get_message", AsyncMock(return_value=query))
    monkeypatch.setattr(worker, "add_callback_id", AsyncMock())
    running = AsyncMock(side_effect=[["cb"], []])
    monkeypatch.setattr(worker, "get_running_callbacks", running)
    monkeypatch.setattr(worker, "abandon_lookup_if_data_lost", AsyncMock())
    cleanup = AsyncMock()
    monkeypatch.setattr(worker, "cleanup_callbacks", cleanup)
    monkeypatch.setattr(worker.asyncio, "sleep", AsyncMock())

    class _Client:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            class _R:
                status_code = 200

            return _R()

    monkeypatch.setattr(worker.httpx, "AsyncClient", _Client)

    await worker.aragorn_lookup(
        ("1-0", {"query_id": "q1", "response_id": "r1", "otel": "{}"}), logger
    )

    assert running.await_count == 2
    cleanup.assert_not_awaited()
