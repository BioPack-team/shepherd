"""Tests for the log level the /callback endpoint runs at.

The level a query asked for lives in the query itself. A TRAPI *response* has
no ``log_level`` field, so the body a subservice posts to ``/callback`` can't
carry it -- the handler has to read it back from the stored query. Getting this
wrong is quiet: everything downstream of the callback (the handler's own logs,
the merge task it enqueues, and the retrieval logs merge folds into the query's
log list) silently runs at INFO and a DEBUG query loses its logs.
"""

import logging

import orjson
import pytest
from starlette.requests import Request

from shepherd_server import base_routes
from shepherd_server.base_routes import ARATargetEnum, callback
from shepherd_utils.db import save_message

logger = logging.getLogger(__name__)


def _make_request(body: bytes) -> Request:
    """A Starlette Request that streams ``body`` in one chunk."""
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/aragorn/callback/cb-1",
        "headers": [(b"content-length", str(len(body)).encode())],
    }
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(scope, receive)


def _patch_callback_deps(monkeypatch):
    """Stub the postgres-backed lookups and capture the enqueued task."""
    tasks = []

    async def _get_callback_query_id(callback_id, logger):
        return ("q-1", "{}")

    async def _get_query_state(query_id, logger):
        # response_id lives at index 7 of the shepherd_brain row.
        return [None, None, None, None, None, None, None, "resp-1"]

    async def _add_ready_callback(response_id, callback_id, logger):
        return None

    async def _add_task(stream, fields, logger):
        tasks.append((stream, fields))

    async def _save_logs(response_id, logger):
        return None

    monkeypatch.setattr(base_routes, "get_callback_query_id", _get_callback_query_id)
    monkeypatch.setattr(base_routes, "get_query_state", _get_query_state)
    monkeypatch.setattr(base_routes, "add_ready_callback", _add_ready_callback)
    monkeypatch.setattr(base_routes, "add_task", _add_task)
    monkeypatch.setattr(base_routes, "save_logs", _save_logs)
    return tasks


def _callback_body(**extra):
    body = {"message": {"results": [], "knowledge_graph": {"nodes": {}, "edges": {}}}}
    body.update(extra)
    return orjson.dumps(body)


@pytest.mark.asyncio
async def test_callback_takes_its_level_from_the_stored_query(redis_mock, monkeypatch):
    """A DEBUG query keeps logging at DEBUG once its callbacks come back."""
    tasks = _patch_callback_deps(monkeypatch)
    await save_message("q-1", {"log_level": "DEBUG", "message": {}}, logger)

    response = await callback(
        ARATargetEnum.ARAGORN, "cb-1", _make_request(_callback_body())
    )

    assert response.status_code == 200
    assert logging.getLogger("shepherd.cb-1").level == logging.DEBUG
    # ...and the level rides along to the merge, which filters the retrieval's
    # own log entries against it.
    assert tasks[0][0] == "merge_message"
    assert tasks[0][1]["log_level"] == logging.DEBUG


@pytest.mark.asyncio
async def test_callback_ignores_a_level_claimed_by_the_body(redis_mock, monkeypatch):
    """The regression: the level used to be read off the posted body. Nothing a
    subservice sends back gets to lower (or raise) what the client asked for."""
    tasks = _patch_callback_deps(monkeypatch)
    await save_message("q-1", {"log_level": "DEBUG", "message": {}}, logger)

    await callback(
        ARATargetEnum.ARAGORN, "cb-2", _make_request(_callback_body(log_level="ERROR"))
    )

    assert logging.getLogger("shepherd.cb-2").level == logging.DEBUG
    assert tasks[0][1]["log_level"] == logging.DEBUG


@pytest.mark.asyncio
async def test_callback_falls_back_to_the_server_default(redis_mock, monkeypatch):
    """A query that didn't ask for a level gets the configured default."""
    tasks = _patch_callback_deps(monkeypatch)
    await save_message("q-1", {"message": {}}, logger)

    await callback(ARATargetEnum.ARAGORN, "cb-3", _make_request(_callback_body()))

    assert logging.getLogger("shepherd.cb-3").level == logging.INFO
    assert tasks[0][1]["log_level"] == logging.INFO


@pytest.mark.asyncio
async def test_callback_survives_an_unreadable_query(redis_mock, monkeypatch):
    """The stored query can have expired out from under a late callback; that
    shouldn't fail the callback, just fall back to the default level."""
    tasks = _patch_callback_deps(monkeypatch)
    # "q-1" intentionally not stored.

    response = await callback(
        ARATargetEnum.ARAGORN, "cb-4", _make_request(_callback_body())
    )

    assert response.status_code == 200
    assert logging.getLogger("shepherd.cb-4").level == logging.INFO
    assert tasks[0][1]["log_level"] == logging.INFO
