"""Tests for how ``workers.aragorn_lookup.worker`` handles the /asyncquery
submission ACK.

A creative query fans out to ~20 submissions at once and the retrieval
service has been seen taking 20s+ to ACK the burst. A submission whose ACK
times out has still been received -- the service runs it and POSTs the
callback later -- so its callback id must survive, or the callback handler
rejects the results with a 500 and the lookup waits out its deadline for
nothing.
"""

import json
import logging

import httpx
import pytest

from workers.aragorn_lookup import worker as al
from workers.aragorn_lookup.worker import (
    AsyncResponse,
    LookupRejectedError,
    aragorn_lookup,
    run_async_lookup,
)

logger = logging.getLogger(__name__)


def _make_task():
    return [
        "test",
        {
            "query_id": "qid",
            "response_id": "rid",
            "workflow": json.dumps([{"id": "aragorn.lookup"}]),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]


INFERRED_MSG = {
    "message": {
        "query_graph": {
            "nodes": {
                "a": {"ids": ["X:1"], "categories": ["biolink:Drug"]},
                "b": {"categories": ["biolink:Disease"]},
            },
            "edges": {
                "e0": {
                    "subject": "a",
                    "object": "b",
                    "knowledge_type": "inferred",
                    "predicates": ["biolink:treats"],
                }
            },
        }
    },
    "parameters": {"timeout": 5},
}


@pytest.mark.asyncio
async def test_run_async_lookup_read_timeout_is_in_flight(redis_mock, mocker):
    mocker.patch.object(al, "add_callback_id", new_callable=mocker.AsyncMock)

    client = mocker.Mock()
    client.timeout.read = 100.0
    client.post = mocker.AsyncMock(side_effect=httpx.ReadTimeout(""))

    out = await run_async_lookup(client, {"message": {}}, "qid", logger)
    assert out.success is False
    assert out.in_flight is True
    assert "ReadTimeout" in out.error
    assert "100.0s" in out.error


@pytest.mark.asyncio
async def test_run_async_lookup_connect_error_is_not_in_flight(redis_mock, mocker):
    mocker.patch.object(al, "add_callback_id", new_callable=mocker.AsyncMock)

    client = mocker.Mock()
    client.post = mocker.AsyncMock(side_effect=httpx.ConnectError("boom"))

    out = await run_async_lookup(client, {"message": {}}, "qid", logger)
    assert out.success is False
    assert out.in_flight is False
    assert "ConnectError: boom" == out.error


@pytest.mark.asyncio
async def test_aragorn_lookup_keeps_in_flight_callback_ids(redis_mock, mocker):
    """Only submissions that definitely failed lose their callback id."""
    mocker.patch.object(
        al, "get_message", new_callable=mocker.AsyncMock, return_value=INFERRED_MSG
    )
    mocker.patch.object(
        al,
        "expand_aragorn_query",
        return_value=[
            {"message": {"query_graph": {}}, "parameters": {}, "submitter": "t"},
            {"message": {"query_graph": {}}, "parameters": {}, "submitter": "t"},
            {"message": {"query_graph": {}}, "parameters": {}, "submitter": "t"},
        ],
    )
    mocker.patch.object(
        al,
        "run_async_lookup",
        new_callable=mocker.AsyncMock,
        side_effect=[
            AsyncResponse(status_code=200, success=True, callback_id="ok-cb"),
            AsyncResponse(
                status_code=500,
                success=False,
                callback_id="slow-ack-cb",
                error="ReadTimeout",
                in_flight=True,
            ),
            AsyncResponse(
                status_code=500, success=False, callback_id="failed-cb", error="x"
            ),
        ],
    )
    mock_remove = mocker.patch.object(
        al, "remove_callback_id", new_callable=mocker.AsyncMock
    )
    mocker.patch.object(
        al, "get_running_callbacks", new_callable=mocker.AsyncMock, return_value=[]
    )
    await aragorn_lookup(_make_task(), logger)
    mock_remove.assert_awaited_once_with("failed-cb", logger)


LOOKUP_MSG = {
    "message": {
        "query_graph": {
            "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
            "edges": {"e0": {"subject": "a", "object": "b"}},
        }
    },
    "parameters": {"timeout": 5},
}


@pytest.mark.asyncio
async def test_rejected_lookup_fails_instead_of_waiting(redis_mock, mocker):
    """A submission the retrieval service refuses never calls back. Waiting on
    its callback id held the query for the whole lookup timeout; it now fails
    straight away with the service's reason."""
    mocker.patch.object(
        al, "get_message", new_callable=mocker.AsyncMock, return_value=LOOKUP_MSG
    )
    mocker.patch.object(al, "add_callback_id", new_callable=mocker.AsyncMock)
    mock_remove = mocker.patch.object(
        al, "remove_callback_id", new_callable=mocker.AsyncMock
    )
    running = mocker.patch.object(
        al, "get_running_callbacks", new_callable=mocker.AsyncMock
    )
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=httpx.Response(422, text='{"detail": "query_graph missing"}'),
    )
    with pytest.raises(LookupRejectedError, match="HTTP 422.*query_graph missing"):
        await aragorn_lookup(_make_task(), logger)
    mock_remove.assert_awaited_once()
    running.assert_not_called()


@pytest.mark.asyncio
async def test_run_async_lookup_rejection_carries_the_reason(redis_mock, mocker):
    mocker.patch.object(al, "add_callback_id", new_callable=mocker.AsyncMock)
    client = mocker.Mock()
    client.post = mocker.AsyncMock(return_value=httpx.Response(400, text="bad qg"))

    out = await run_async_lookup(client, {"message": {}}, "qid", logger)
    assert out.success is False
    assert out.in_flight is False
    assert out.error == "HTTP 400 bad qg"
