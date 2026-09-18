"""Tests for the /callback request-size limit.

The callback endpoint buffers the posted TRAPI response into memory, so an
oversized payload can OOM the server (and, once merged, the downstream workers).
``callback`` rejects anything larger than ``callback_max_request_size`` with a
413 before the whole body is read.
"""

import logging

import pytest
import zstandard
from starlette.requests import Request

from shepherd_server import base_routes
from shepherd_server.base_routes import ARATargetEnum, _read_body_within_limit, callback
from shepherd_utils.config import settings


def _make_request(body: bytes, headers: dict, chunk_size: int | None = None) -> Request:
    """Build a Starlette Request that streams ``body`` from an ASGI receive.

    ``chunk_size`` splits the body across multiple ``http.request`` messages so
    the streaming-abort path can be exercised; ``None`` sends it in one chunk.
    """
    header_list = [(k.lower().encode(), str(v).encode()) for k, v in headers.items()]
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/callback/abc",
        "headers": header_list,
    }
    if chunk_size:
        chunks = [body[i : i + chunk_size] for i in range(0, len(body), chunk_size)]
    else:
        chunks = [body]
    if not chunks:
        chunks = [b""]
    messages = [
        {"type": "http.request", "body": c, "more_body": i < len(chunks) - 1}
        for i, c in enumerate(chunks)
    ]
    it = iter(messages)

    async def receive():
        try:
            return next(it)
        except StopIteration:
            return {"type": "http.disconnect"}

    return Request(scope, receive)


async def test_reads_full_body_when_under_limit():
    body = b'{"message": {}}'
    request = _make_request(body, {"content-length": len(body)})
    assert await _read_body_within_limit(request, 1000) == body


async def test_rejects_on_declared_content_length():
    # Fast path: the declared Content-Length alone is over the limit, so we
    # reject without reading the (here intentionally absent) body.
    request = _make_request(b"", {"content-length": 5000})
    assert await _read_body_within_limit(request, 1000) is None


async def test_rejects_when_stream_exceeds_limit_without_content_length():
    # No Content-Length header: the cap must still trip mid-stream.
    body = b"x" * 5000
    request = _make_request(body, {}, chunk_size=512)
    assert await _read_body_within_limit(request, 1000) is None


async def test_lying_content_length_is_caught_by_stream_cap():
    # Content-Length under-reports; the streaming total still enforces the cap.
    body = b"x" * 5000
    request = _make_request(body, {"content-length": 10}, chunk_size=512)
    assert await _read_body_within_limit(request, 1000) is None


async def test_zero_limit_disables_the_cap():
    body = b"x" * 5000
    request = _make_request(body, {"content-length": len(body)})
    assert await _read_body_within_limit(request, 0) == body


def _resolve_query(monkeypatch, calls):
    """Point the callback->query mapping at q-1 / resp-1 and capture the
    query-failure call, so the tests can assert on it without Postgres."""

    async def _fake_get_callback_query_id(callback_id, logger):
        calls.append(("resolve_query", callback_id))
        return ("q-1", "{}")

    async def _fake_get_query_state(query_id, logger):
        # response_id lives at index 7 of the shepherd_brain row.
        return [None, None, None, None, None, None, None, "resp-1"]

    async def _fake_fail(query_id, response_id, reason, logger):
        calls.append(("fail", query_id, response_id, reason))

    monkeypatch.setattr(
        base_routes, "get_callback_query_id", _fake_get_callback_query_id
    )
    monkeypatch.setattr(base_routes, "get_query_state", _fake_get_query_state)
    monkeypatch.setattr(base_routes, "fail_response_too_large", _fake_fail)


async def test_callback_returns_413_and_fails_the_query_for_oversized_payload(
    monkeypatch,
):
    """An oversized callback is a response too big to build: the whole query
    is failed as RESPONSE_TOO_LARGE rather than just this callback dropped,
    which would have delivered an answer silently missing most of its data."""
    monkeypatch.setattr(settings, "callback_max_request_size", "1000")
    calls = []
    _resolve_query(monkeypatch, calls)

    body = b"x" * 5000
    request = _make_request(body, {"content-length": len(body)})

    response = await callback(ARATargetEnum.ARAGORN, "cb-1", request)

    assert response.status_code == 413
    fails = [c for c in calls if c[0] == "fail"]
    assert len(fails) == 1
    _, query_id, response_id, reason = fails[0]
    assert (query_id, response_id) == ("q-1", "resp-1")
    # The reason names the callback and the cap, so the log line and the
    # delivered description say what happened.
    assert "cb-1" in reason
    assert "1000" in reason


async def test_callback_413_logs_critical_when_query_is_unknown(monkeypatch, caplog):
    """With no callback->query mapping there is no query to fail, but the
    rejection must still be impossible to miss: CRITICAL, with the marker."""
    monkeypatch.setattr(settings, "callback_max_request_size", "1000")

    async def _no_mapping(callback_id, logger):
        return None

    monkeypatch.setattr(base_routes, "get_callback_query_id", _no_mapping)

    body = b"x" * 5000
    request = _make_request(body, {"content-length": len(body)})

    with caplog.at_level(logging.CRITICAL):
        response = await callback(ARATargetEnum.ARAGORN, "cb-1", request)

    assert response.status_code == 413
    critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert critical, "the rejection must be logged at CRITICAL"
    assert "RESPONSE_TOO_LARGE" in critical[0].getMessage()
    assert "cb-1" in critical[0].getMessage()


async def test_callback_413_for_zstd_body_that_decompresses_past_the_cap(
    monkeypatch,
):
    """The cap is about what has to be held in memory, so a small compressed
    body that expands past it is rejected on its decompressed size."""
    monkeypatch.setattr(settings, "callback_max_request_size", "1000")
    calls = []
    _resolve_query(monkeypatch, calls)

    # 100KB of a single byte compresses to well under the 1000-byte wire cap.
    body = zstandard.compress(b"x" * 100_000)
    assert len(body) < 1000
    request = _make_request(
        body, {"content-length": len(body), "content-encoding": "zstd"}
    )

    response = await callback(ARATargetEnum.ARAGORN, "cb-1", request)

    assert response.status_code == 413
    fails = [c for c in calls if c[0] == "fail"]
    assert len(fails) == 1
    assert "decompresses" in fails[0][3]


async def test_callback_422_persists_logs_on_invalid_body(monkeypatch):
    """An unparseable body is also a callback error whose log must be saved."""
    monkeypatch.setattr(settings, "callback_max_request_size", "100000")
    saved = []

    async def _fake_get_callback_query_id(callback_id, logger):
        return ("q-1", "{}")

    async def _fake_get_query_state(query_id, logger):
        return [None, None, None, None, None, None, None, "resp-1"]

    async def _fake_save_logs(response_id, logger):
        saved.append(response_id)

    monkeypatch.setattr(
        base_routes, "get_callback_query_id", _fake_get_callback_query_id
    )
    monkeypatch.setattr(base_routes, "get_query_state", _fake_get_query_state)
    monkeypatch.setattr(base_routes, "save_logs", _fake_save_logs)

    body = b"this is not json"
    request = _make_request(body, {"content-length": len(body)})

    response = await callback(ARATargetEnum.ARAGORN, "cb-1", request)

    assert response.status_code == 422
    assert saved == ["resp-1"]
