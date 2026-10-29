"""Tests for the /query and /asyncquery request-body limits (issue #183).

``parse_query_body`` reads the body through ``_read_body_within_limit`` against
``query_max_request_size`` (413 when over), and gives up on a body that stops
arriving after ``request_body_idle_timeout_sec`` (408) -- the slowloris case a
size cap alone never catches, since a stalled stream never crosses it.
"""

import asyncio
import json
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

from shepherd_server import base_routes
from shepherd_server.aras.aragorn import ARAGORN
from shepherd_server.aras.arax import ARAX
from shepherd_server.base_routes import (
    BodyReadTimeout,
    QueryBodyTooLarge,
    _read_body_within_limit,
    default_input_query,
    parse_query_body,
)
from shepherd_utils.config import settings

from .test_callback_size_limit import _make_request


def _stalling_request(first_chunk: bytes, declared_length: int) -> Request:
    """A request that sends one chunk, claims more is coming, then goes quiet."""
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/query",
        "headers": [
            (b"content-type", b"application/json"),
            (b"content-length", str(declared_length).encode()),
        ],
    }
    sent = False

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": first_chunk, "more_body": True}
        await asyncio.Event().wait()  # never arrives

    return Request(scope, receive)


@pytest.fixture
def limits(monkeypatch):
    def _set(size="1000", idle=30.0):
        monkeypatch.setattr(settings, "query_max_request_size", size)
        monkeypatch.setattr(settings, "request_body_idle_timeout_sec", idle)

    return _set


async def test_stalled_body_times_out():
    request = _stalling_request(b'{"message"', 10**9)
    with pytest.raises(BodyReadTimeout, match="no data received"):
        await _read_body_within_limit(request, 0, idle_timeout=0.05)


async def test_stalled_body_times_out_under_the_size_cap():
    # The cap alone never trips: the stream just stops.
    request = _stalling_request(b"x" * 10, 500)
    with pytest.raises(BodyReadTimeout):
        await _read_body_within_limit(request, 1000, idle_timeout=0.05)


async def test_read_body_is_cached_for_a_second_read():
    body = json.dumps(default_input_query).encode()
    request = _make_request(body, {"content-length": len(body)}, chunk_size=7)
    assert await _read_body_within_limit(request, 0, idle_timeout=1.0) == body
    assert await request.body() == body


async def test_parse_query_body_rejects_a_declared_oversize(limits):
    limits(size="1000")
    request = _make_request(
        b"", {"content-type": "application/json", "content-length": 10**9}
    )
    with pytest.raises(QueryBodyTooLarge, match="1000 bytes"):
        await parse_query_body(request)


async def test_parse_query_body_rejects_an_undeclared_oversize(limits):
    limits(size="1000")
    request = _make_request(
        b" " * 5000, {"content-type": "application/json"}, chunk_size=512
    )
    with pytest.raises(QueryBodyTooLarge):
        await parse_query_body(request)


async def test_parse_query_body_under_the_cap_is_unchanged(limits):
    limits(size="1MB")
    body = json.dumps(default_input_query).encode()
    request = _make_request(body, {"content-type": "application/json"})
    assert (await parse_query_body(request))["message"]["query_graph"]


async def test_zero_disables_the_cap(limits):
    limits(size="0")
    body = json.dumps(default_input_query).encode() + b" " * 5000
    request = _make_request(body, {"content-type": "application/json"}, chunk_size=512)
    assert await parse_query_body(request)


def test_default_cap_is_on():
    assert settings.query_max_request_size_bytes > 0
    assert settings.request_body_idle_timeout_sec > 0


@pytest.fixture
def stubbed_intake():
    async def fake_run_query(target, query, callback_url=None):
        raise AssertionError("an oversized body must not reach intake")

    with mock.patch.object(base_routes, "run_query", fake_run_query):
        yield


@pytest.mark.parametrize(
    "app, path",
    [(ARAGORN, "/query"), (ARAGORN, "/asyncquery"), (ARAX, "/query")],
)
def test_oversized_body_is_413(app, path, limits, stubbed_intake):
    limits(size="1000")
    body = dict(default_input_query, callback="http://callback/1")
    body["padding"] = "x" * 5000
    resp = TestClient(app).post(path, json=body)
    assert resp.status_code == 413, resp.text
    assert "maximum allowed size" in resp.json()["description"]


@pytest.mark.parametrize("path", ["/query", "/asyncquery"])
async def test_stalled_body_is_408(path, limits, stubbed_intake):
    limits(size="1MB", idle=0.05)
    handler = (
        base_routes.run_sync_query if path == "/query" else base_routes.run_async_query
    )
    resp = await handler("aragorn", _stalling_request(b'{"message"', 10**6))
    assert resp.status_code == 408
