"""Tests for the /callback request-size limit.

The callback endpoint buffers the posted TRAPI response into memory, so an
oversized payload can OOM the server (and, once merged, the downstream workers).
``callback`` rejects anything larger than ``callback_max_request_size`` with a
413 before the whole body is read.
"""

import pytest
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


async def test_callback_returns_413_and_drops_callback_for_oversized_payload(
    monkeypatch,
):
    monkeypatch.setattr(settings, "callback_max_request_size", "1000")
    removed = []

    async def _fake_remove(callback_id, logger):
        removed.append(callback_id)

    # Patch the name as imported into base_routes.
    monkeypatch.setattr(base_routes, "remove_callback_id", _fake_remove)

    body = b"x" * 5000
    request = _make_request(body, {"content-length": len(body)})

    response = await callback(ARATargetEnum.ARAGORN, "cb-1", request)

    assert response.status_code == 413
    # The rejected callback must be dropped from the running set so the lookup
    # worker doesn't hang waiting for it until its timeout.
    assert removed == ["cb-1"]
