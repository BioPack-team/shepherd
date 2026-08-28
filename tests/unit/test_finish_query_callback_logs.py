"""Tests for the callback timing/failure logging in ``finish_query``.

Covers what the delivery logs have to answer after the fact: how long the
callback POST took, and -- when it didn't land -- why, both in the worker's
logs and in the payload the receiver eventually gets.
"""

import json
import logging

import httpx
import orjson
import pytest

from workers.finish_query.worker import (
    CALLBACK_ERROR_BODY_BYTES,
    CALLBACK_RETRIES,
    _append_log_entry,
    _describe_callback_failure,
    finish_query,
)

logger = logging.getLogger(__name__)

TASK = [
    "test",
    {
        "query_id": "test",
        "response_id": "rid",
        "workflow": json.dumps([]),
        "log_level": "20",
    },
]


def _patch_async_query(mocker, message=None, logs=None):
    """Patch the db reads for an async (callback) query."""
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
        "workers.finish_query.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=orjson.dumps(message if message is not None else {"message": {}}),
    )
    mocker.patch(
        "workers.finish_query.worker.get_logs",
        new_callable=mocker.AsyncMock,
        return_value=logs if logs is not None else [],
    )


def _http_error_response(status_code: int, body: bytes) -> httpx.Response:
    """A real httpx response, so ``raise_for_status`` raises the real error."""
    return httpx.Response(
        status_code,
        content=body,
        request=httpx.Request("POST", "http://callback"),
    )


@pytest.mark.asyncio
async def test_successful_callback_logs_duration_and_size(redis_mock, mocker, caplog):
    """A delivered callback logs how long the POST took and how big it was."""
    _patch_async_query(mocker)
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=_http_error_response(200, b"ok"),
    )

    with caplog.at_level(logging.INFO):
        await finish_query(TASK, logger)

    sent = [r.message for r in caplog.records if "Sent response back" in r.message]
    assert len(sent) == 1
    assert "http://callback" in sent[0]
    # "<n>.<mmm>s" duration and a byte count, both after the send completed.
    assert "s (" in sent[0] and "bytes" in sent[0]
    assert f"attempt 1/{CALLBACK_RETRIES}" in sent[0]


@pytest.mark.asyncio
async def test_failed_callback_logs_status_and_body(redis_mock, mocker, caplog):
    """A rejected callback logs the status code and the server's explanation."""
    _patch_async_query(mocker)
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=_http_error_response(502, b"upstream exploded"),
    )
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)

    with caplog.at_level(logging.INFO):
        await finish_query(TASK, logger)

    failures = [
        r.message for r in caplog.records if "Failed to send callback" in r.message
    ]
    assert len(failures) == CALLBACK_RETRIES
    assert "HTTP 502" in failures[0]
    assert "upstream exploded" in failures[0]
    assert f"attempt 1/{CALLBACK_RETRIES}" in failures[0]
    # And a single summary line once the retries are spent.
    gave_up = [
        r.message for r in caplog.records if "Gave up sending callback" in r.message
    ]
    assert len(gave_up) == 1
    assert "http://callback" in gave_up[0]


@pytest.mark.asyncio
async def test_failure_is_spliced_into_the_retry_payload(redis_mock, mocker):
    """The receiver sees the attempts that didn't make it.

    A callback that fails and is retried can't carry its own failure, but it
    can carry the previous attempt's -- so the eventual recipient knows the
    response is late and why.
    """
    _patch_async_query(mocker, logs=[{"message": "earlier", "level": "INFO"}])
    payloads = []

    async def record(*args, **kwargs):
        payloads.append(kwargs["content"])
        if len(payloads) < 3:
            return _http_error_response(503, b"try again later")
        return _http_error_response(200, b"ok")

    mocker.patch("httpx.AsyncClient.post", side_effect=record)
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)

    await finish_query(TASK, logger)

    assert len(payloads) == 3
    first, second, third = (orjson.loads(p) for p in payloads)
    assert [entry["message"] for entry in first["logs"]] == ["earlier"]
    assert len(second["logs"]) == 2
    assert "HTTP 503" in second["logs"][1]["message"]
    assert second["logs"][1]["level"] == "ERROR"
    assert second["logs"][1]["timestamp"]
    # Each attempt adds only its own failure, and the message survives intact.
    assert len(third["logs"]) == 3
    assert third["message"] == {}


@pytest.mark.asyncio
async def test_oversized_payload_skips_the_inline_retry_note(redis_mock, mocker):
    """A huge payload isn't rebuilt just to carry a note that's in the logs."""
    _patch_async_query(mocker)
    mocker.patch("workers.finish_query.worker.RETRY_LOG_SPLICE_MAX_BYTES", 1)
    payloads = []

    async def record(*args, **kwargs):
        payloads.append(kwargs["content"])
        return _http_error_response(500, b"nope")

    mocker.patch("httpx.AsyncClient.post", side_effect=record)
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)

    await finish_query(TASK, logger)

    assert len(payloads) == CALLBACK_RETRIES
    assert len(set(payloads)) == 1


@pytest.mark.asyncio
async def test_callback_logs_are_persisted_for_the_query(redis_mock, mocker):
    """The delivery outcome lands in the query's logs, not just stdout.

    ``finish_query`` acks directly instead of going through ``wrap_up_task``,
    so it has to flush its own logs -- otherwise a failed callback is invisible
    to anyone reading the query back.
    """
    _patch_async_query(mocker)
    mock_save_logs = mocker.patch(
        "workers.finish_query.worker.save_logs",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=_http_error_response(200, b"ok"),
    )

    await finish_query(TASK, logger)

    mock_save_logs.assert_awaited_once_with("rid", logger)


@pytest.mark.asyncio
async def test_failing_to_save_logs_does_not_fail_the_task(redis_mock, mocker):
    """A log flush that blows up must not take the whole wrap-up with it."""
    _patch_async_query(mocker)
    mocker.patch(
        "workers.finish_query.worker.save_logs",
        new_callable=mocker.AsyncMock,
        side_effect=Exception("redis down"),
    )
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=_http_error_response(200, b"ok"),
    )

    await finish_query(TASK, logger)


def test_describe_callback_failure_truncates_the_body():
    """A rejecting server can return anything; only the head of it is logged."""
    error = httpx.HTTPStatusError(
        "boom",
        request=httpx.Request("POST", "http://callback"),
        response=_http_error_response(413, b"x" * (CALLBACK_ERROR_BODY_BYTES * 10)),
    )
    described = _describe_callback_failure(error)
    assert described.startswith("HTTP 413: ")
    assert len(described) < CALLBACK_ERROR_BODY_BYTES + 50


def test_describe_callback_failure_names_silent_exceptions():
    """httpx's connect/timeout errors often stringify to nothing useful."""
    assert "ConnectTimeout" in _describe_callback_failure(httpx.ConnectTimeout(""))
    assert "ConnectError" in _describe_callback_failure(httpx.ConnectError(""))


def test_append_log_entry_handles_both_array_shapes():
    """Empty and populated logs arrays both stay valid JSON."""
    entry = {"message": "late", "level": "ERROR"}
    empty = orjson.dumps({"message": {}, "logs": []})
    assert orjson.loads(_append_log_entry(empty, entry))["logs"] == [entry]

    populated = orjson.dumps({"message": {}, "logs": [{"message": "first"}]})
    appended = orjson.loads(_append_log_entry(populated, entry))["logs"]
    assert [e["message"] for e in appended] == ["first", "late"]


def test_append_log_entry_leaves_an_unexpected_tail_alone():
    """Rather than corrupt a payload we don't recognize, send it as-is."""
    payload = orjson.dumps({"logs": [], "message": {}})
    assert _append_log_entry(payload, {"message": "late"}) == payload
