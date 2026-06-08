"""Additional ``workers.finish_query.worker`` tests covering paths the
existing happy-path tests don't reach.
"""

import json
import logging

import orjson
import pytest

from workers.finish_query.worker import finish_query

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_finish_query_skips_callback_when_state_missing(redis_mock, mocker):
    """If get_query_state returns None, don't try to fetch a message or POST.

    The query is not marked completed in this branch either -- nothing in the
    db to update.
    """
    mock_query_state = mocker.patch(
        "workers.finish_query.worker.get_query_state",
        new_callable=mocker.AsyncMock,
        return_value=None,
    )
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed",
        new_callable=mocker.AsyncMock,
    )
    mock_get_message = mocker.patch(
        "workers.finish_query.worker.get_message_raw",
        new_callable=mocker.AsyncMock,
    )
    mock_post = mocker.patch("httpx.AsyncClient.post", new_callable=mocker.AsyncMock)

    await finish_query(
        [
            "test",
            {
                "query_id": "ghost",
                "response_id": "ignored",
                "workflow": json.dumps([]),
                "log_level": "20",
            },
        ],
        logger,
    )

    assert mock_query_state.called
    assert not mock_set_query_completed.called
    assert not mock_get_message.called
    assert not mock_post.called


@pytest.mark.asyncio
async def test_finish_query_propagates_status_to_set_query_completed(
    redis_mock, mocker
):
    """An ERROR status (set on a failure-routed task) should be passed along
    to set_query_completed."""
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        new_callable=mocker.AsyncMock,
        return_value=["", "", "", "", "", "", "", "rid", None],  # sync query
    )
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.finish_query.worker.get_message_raw",
        new_callable=mocker.AsyncMock,
        return_value=orjson.dumps({"message": {}}),
    )

    await finish_query(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "rid",
                "workflow": json.dumps([]),
                "log_level": "20",
                "status": "ERROR",
            },
        ],
        logger,
    )

    mock_set_query_completed.assert_called_once_with("test", "ERROR", logger)


@pytest.mark.asyncio
async def test_finish_async_query_retries_callback_on_failure(redis_mock, mocker):
    """If the first POST raises, finish_query should retry up to CALLBACK_RETRIES
    times with backoff before giving up and still mark the query completed."""
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        new_callable=mocker.AsyncMock,
        return_value=["", "", "", "", "", "", "", "rid", "http://callback"],
    )
    mock_set_query_completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.finish_query.worker.get_message_raw",
        new_callable=mocker.AsyncMock,
        return_value=orjson.dumps({"message": {"results": []}}),
    )
    mocker.patch(
        "workers.finish_query.worker.get_logs",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )

    mock_post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        side_effect=Exception("simulated network error"),
    )
    # Don't actually sleep between retries.
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)

    await finish_query(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "rid",
                "workflow": json.dumps([]),
                "log_level": "20",
            },
        ],
        logger,
    )
    # 3 retries baked into the worker.
    assert mock_post.call_count == 3
    assert mock_set_query_completed.called


@pytest.mark.asyncio
async def test_finish_async_query_attaches_logs_to_message_payload(redis_mock, mocker):
    """The async callback POST should send the message with logs attached."""
    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        new_callable=mocker.AsyncMock,
        return_value=["", "", "", "", "", "", "", "rid", "http://callback"],
    )
    mocker.patch(
        "workers.finish_query.worker.set_query_completed",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.finish_query.worker.get_message_raw",
        new_callable=mocker.AsyncMock,
        return_value=orjson.dumps({"message": {"results": []}}),
    )
    mocker.patch(
        "workers.finish_query.worker.get_logs",
        new_callable=mocker.AsyncMock,
        return_value=[
            {"message": "log line", "timestamp": "ts", "level": "INFO"},
        ],
    )
    mock_response = mocker.Mock()
    mock_response.raise_for_status = mocker.Mock()
    mock_post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mock_response,
    )

    await finish_query(
        [
            "test",
            {
                "query_id": "test",
                "response_id": "rid",
                "workflow": json.dumps([]),
                "log_level": "20",
            },
        ],
        logger,
    )
    assert mock_post.called
    posted_payload = orjson.loads(mock_post.call_args.kwargs["content"])
    assert "logs" in posted_payload
    assert posted_payload["logs"][0]["message"] == "log line"
