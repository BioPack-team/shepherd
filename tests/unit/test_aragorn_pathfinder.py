"""Tests for ``workers.aragorn_pathfinder.worker.shadowfax``.

The pathfinder entry point validates the input pathfinder query, sends
one-hop edge lookups to Retriever, waits for their callbacks, and builds the
paths in a process pool. We exercise the validation paths and the happy path
(mocking out postgres, redis and the pool).
"""

import json
import logging

import pytest

from workers.aragorn_pathfinder import worker as pathfinder_worker
from workers.aragorn_pathfinder.worker import shadowfax

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def pool(mocker):
    """Stands in for the process pool poll_for_tasks sets up: the path build
    is not run, only the lookups that feed it."""
    pool = mocker.Mock()
    pool.run = mocker.AsyncMock(return_value=[])
    mocker.patch.object(pathfinder_worker, "POOL", pool)
    return pool


def _make_task(workflow=None):
    return [
        "test",
        {
            "query_id": "qid-1",
            "response_id": "rid-1",
            "workflow": json.dumps(workflow if workflow is not None else []),
            "log_level": "20",
            "otel": json.dumps({}),
            "metadata": json.dumps({}),
        },
    ]


def _pathfinder_message(constraints=None):
    msg = {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"ids": ["MONDO:0001"]},
                    "n1": {"ids": ["MONDO:0002"]},
                },
                "paths": {
                    "p0": {"subject": "n0", "object": "n1"},
                },
            }
        },
        "parameters": {"timeout": 5},
    }
    if constraints is not None:
        msg["message"]["query_graph"]["paths"]["p0"]["constraints"] = constraints
    return msg


@pytest.mark.asyncio
async def test_shadowfax_happy_path_builds_paths(redis_mock, mocker, pool):
    """A valid pathfinder query: callback registered, edge lookup sent, the
    wait exits when no callbacks remain, and the paths are built in the pool."""
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=_pathfinder_message(),
    )
    mock_add_cb = mocker.patch(
        "workers.aragorn_pathfinder.worker.add_callback_id",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_httpx = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mock_response,
    )

    await shadowfax(_make_task(), logger)

    assert mock_add_cb.called
    assert mock_httpx.called
    pool.run.assert_awaited_once()


@pytest.mark.asyncio
async def test_shadowfax_requires_two_distinct_pinned_nodes(redis_mock, mocker):
    """Only one distinct pinned id is invalid for a pathfinder query."""
    msg = _pathfinder_message()
    msg["message"]["query_graph"]["nodes"]["n1"]["ids"] = ["MONDO:0001"]  # duplicate
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    with pytest.raises(Exception, match="two pinned nodes"):
        await shadowfax(_make_task(), logger)


@pytest.mark.asyncio
async def test_shadowfax_rejects_multiple_constraints(redis_mock, mocker):
    """Multiple constraints on the path is unsupported."""
    msg = _pathfinder_message(
        constraints=[
            {"intermediate_categories": ["biolink:Gene"]},
            {"intermediate_categories": ["biolink:Disease"]},
        ]
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    with pytest.raises(Exception, match="multiple constraints"):
        await shadowfax(_make_task(), logger)


@pytest.mark.asyncio
async def test_shadowfax_rejects_multiple_intermediate_categories(redis_mock, mocker):
    """A single constraint may not list multiple intermediate categories."""
    msg = _pathfinder_message(
        constraints=[{"intermediate_categories": ["biolink:Gene", "biolink:Disease"]}]
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    with pytest.raises(Exception, match="multiple intermediate categories"):
        await shadowfax(_make_task(), logger)


@pytest.mark.asyncio
async def test_shadowfax_sends_its_own_lookup_parameters(redis_mock, mocker):
    """The edge lookups carry the worker's own retrieval parameters (one
    tier, no tier fallback, dehydrated), not the caller's 3-hop filters."""
    msg = _pathfinder_message()
    msg["parameters"]["filter_config"] = {
        "min_information_content": 1,
        "max_node_degree": 10,
    }
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.add_callback_id",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_httpx = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mock_response,
    )

    await shadowfax(_make_task(), logger)

    mock_httpx.assert_awaited_once()

    args, kwargs = mock_httpx.call_args

    query_parameters = kwargs["json"]["parameters"]
    assert "filter_config" not in query_parameters
    assert query_parameters["timeout"] == 5
    assert query_parameters["tier_fallback"] is False
    assert query_parameters["dehydrated"] is True


@pytest.mark.asyncio
async def test_shadowfax_accepts_an_intermediate_category(redis_mock, mocker):
    """A path constraint (intermediate_categories) is accepted, and the query's
    log_level is not passed on to the edge lookups sent to Retriever."""
    msg = _pathfinder_message(
        constraints=[{"intermediate_categories": ["biolink:Gene"]}]
    )
    msg["log_level"] = "DEBUG"
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.add_callback_id",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.aragorn_pathfinder.worker.get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_httpx = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mock_response,
    )

    await shadowfax(_make_task(), logger)

    sent = mock_httpx.call_args.kwargs["json"]
    assert "log_level" not in sent
    assert "log_level" not in sent["parameters"]
