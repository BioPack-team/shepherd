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
    _CallbackTrace,
    _append_log_entry,
    _describe_callback_failure,
    _format_phases,
    _phase_attributes,
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


class _FakeSpan:
    """Records what the worker puts on the current span.

    The worker reaches for the ambient span rather than being handed one, so
    the cheapest way to see what it reported is to be that span.
    """

    def __init__(self):
        self.attributes = {}
        self.events = []

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def add_event(self, name, attributes=None):
        self.events.append((name, attributes or {}))


def _patch_span(mocker) -> _FakeSpan:
    span = _FakeSpan()
    mocker.patch(
        "workers.finish_query.worker.get_current_span",
        return_value=span,
    )
    return span


# The phases httpcore reports for a plain HTTP/1.1 POST that reaches a server,
# in the order it reports them.
_HTTP_PHASE_EVENTS = (
    "connection.connect_tcp",
    "http11.send_request_headers",
    "http11.send_request_body",
    "http11.receive_response_headers",
    "http11.receive_response_body",
)


async def _drive_trace_hook(kwargs, stems=_HTTP_PHASE_EVENTS, failed_after=None):
    """Emit the httpcore trace events the real transport would for one POST.

    ``failed_after`` names the phase whose ``.failed`` event ends the attempt --
    a connect that never lands, say -- so a test can exercise a request that
    died mid-flight without a real socket.
    """
    hook = kwargs["extensions"]["trace"]
    for stem in stems:
        await hook(f"{stem}.started", {})
        if failed_after is not None and stem == failed_after:
            await hook(f"{stem}.failed", {"exception": httpx.ConnectError("")})
            return
        await hook(f"{stem}.complete", {"return_value": None})


@pytest.mark.asyncio
async def test_callback_trace_times_each_phase():
    """Every phase of the POST is timed separately, from httpcore's events."""
    # Halves and quarters, so the expected millisecond counts are exact rather
    # than a float-rounding artifact of the test's own arithmetic.
    ticks = iter([0.0, 0.5, 0.5, 1.5, 1.5, 1.75, 1.75, 2.0, 2.0, 3.0, 3.0, 3.25])
    trace = _CallbackTrace(clock=lambda: next(ticks))

    for stem in (
        "connection.connect_tcp",
        "connection.start_tls",
        "http11.send_request_headers",
        "http11.send_request_body",
        "http11.receive_response_headers",
        "http11.receive_response_body",
    ):
        await trace(f"{stem}.started", {})
        await trace(f"{stem}.complete", {})
    # Events we don't time must not consume the clock either.
    await trace("http11.response_closed.started", {})
    await trace("http11.response_closed.complete", {})

    assert _phase_attributes(trace.phases) == {
        "connect_ms": 500,
        "tls_ms": 1000,
        # Headers and body are both "send", so their times add up.
        "send_ms": 500,
        "wait_ms": 1000,
        "receive_ms": 250,
    }


@pytest.mark.asyncio
async def test_callback_trace_counts_the_time_a_phase_took_to_fail():
    """A phase that failed still burned that time -- that's where it went."""
    ticks = iter([0.0, 30.0])
    trace = _CallbackTrace(clock=lambda: next(ticks))

    await trace("connection.connect_tcp.started", {})
    await trace("connection.connect_tcp.failed", {"exception": httpx.ConnectError("")})

    assert _phase_attributes(trace.phases) == {"connect_ms": 30000}


@pytest.mark.asyncio
async def test_span_carries_the_phase_breakdown_of_the_delivering_attempt(
    redis_mock, mocker, caplog
):
    """A slow callback says where the time went, not just how much there was."""
    _patch_async_query(mocker)
    span = _patch_span(mocker)

    async def post(*args, **kwargs):
        await _drive_trace_hook(kwargs)
        return _http_error_response(200, b"ok")

    mocker.patch("httpx.AsyncClient.post", side_effect=post)

    with caplog.at_level(logging.INFO):
        await finish_query(TASK, logger)

    # Phases that ran are reported; TLS, which plain HTTP never does, isn't.
    for phase in ("connect", "send", "wait", "receive"):
        assert isinstance(span.attributes[f"callback.{phase}_ms"], int)
    assert "callback.tls_ms" not in span.attributes
    assert span.attributes["callback.delivered"] is True
    assert span.attributes["callback.host"] == "callback"
    # And the same breakdown rides along in the query's own logs.
    sent = next(r.message for r in caplog.records if "Sent response back" in r.message)
    assert "connect " in sent and "wait " in sent


@pytest.mark.asyncio
async def test_failed_attempt_event_carries_its_own_phase_breakdown(
    redis_mock, mocker, caplog
):
    """Each attempt's timings survive on its event, not just the last one's."""
    _patch_async_query(mocker)
    span = _patch_span(mocker)
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)

    async def post(*args, **kwargs):
        await _drive_trace_hook(kwargs, failed_after="connection.connect_tcp")
        raise httpx.ConnectError("")

    mocker.patch("httpx.AsyncClient.post", side_effect=post)

    with caplog.at_level(logging.INFO):
        await finish_query(TASK, logger)

    failures = [
        event for name, event in span.events if name == "callback_attempt_failed"
    ]
    assert len(failures) == CALLBACK_RETRIES
    assert [event["attempt"] for event in failures] == [1, 2, 3]
    # A callback that never connects spends all its time in connect, and says so.
    assert all("connect_ms" in event for event in failures)
    assert all("send_ms" not in event for event in failures)
    assert span.attributes["callback.delivered"] is False
    logged = [
        r.message for r in caplog.records if "Failed to send callback" in r.message
    ]
    assert "connect " in logged[0]


@pytest.mark.asyncio
async def test_untimed_attempt_reports_no_phases(redis_mock, mocker, caplog):
    """An attempt that never reached the network reports nothing about phases.

    A row of "connect 0.000s" entries would read as a measurement rather than
    the absence of one.
    """
    _patch_async_query(mocker)
    span = _patch_span(mocker)
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=_http_error_response(200, b"ok"),
    )

    with caplog.at_level(logging.INFO):
        await finish_query(TASK, logger)

    assert not [
        key for key in span.attributes if key.endswith("_ms") and "duration" not in key
    ]
    sent = next(r.message for r in caplog.records if "Sent response back" in r.message)
    assert "connect" not in sent


def test_format_phases_reports_in_request_order():
    """Read top to bottom, the breakdown walks the request from start to end."""
    phases = {"receive": 0.004, "connect": 1.5, "wait": 12.0}
    assert _format_phases(phases) == "connect 1.500s, wait 12.000s, receive 0.004s"
    assert _format_phases({}) == ""
