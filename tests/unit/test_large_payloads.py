"""Large payloads stay off the event loop and inside a memory budget (#217)."""

import asyncio
import logging
import threading
from unittest.mock import AsyncMock

import orjson
import pytest

from shepherd_server import base_routes
from shepherd_utils import config, db
from shepherd_utils.config import settings
from workers.finish_query import worker as finish

logger = logging.getLogger(__name__)


# --- C1-6: get_message ------------------------------------------------------


async def test_a_large_blob_is_decompressed_off_the_event_loop(redis_mock, monkeypatch):
    monkeypatch.setattr(db, "OFF_LOOP_DECOMPRESS_BYTES", 10, raising=False)
    await db.save_message("big", {"message": {"pad": "x" * 5000}}, logger)
    loop_thread = threading.get_ident()
    threads = []
    real = db.decompress_frame

    def _spy(blob):
        threads.append(threading.get_ident())
        return real(blob)

    monkeypatch.setattr(db, "decompress_frame", _spy)

    assert (await db.get_message("big", logger))["message"]["pad"] == "x" * 5000
    assert await db.get_message("big", logger, raw=True)

    assert threads and loop_thread not in threads


# --- C3-10: GET /response/{id} ------------------------------------------------


async def test_the_response_route_serves_the_stored_bytes(redis_mock, monkeypatch):
    await db.save_response("resp-1", {"message": {"results": []}}, logger)
    monkeypatch.setattr(
        base_routes,
        "get_response_query_parameters",
        AsyncMock(return_value={"log_level": "DEBUG"}),
    )
    monkeypatch.setattr(
        base_routes,
        "get_logs",
        AsyncMock(return_value=[{"message": "hi", "level": "INFO"}]),
    )
    real = base_routes.get_message
    raw_flags = []

    async def _spy(message_id, _logger, raw=False):
        raw_flags.append(raw)
        return await real(message_id, _logger, raw=raw)

    monkeypatch.setattr(base_routes, "get_message", _spy)

    response = await base_routes.get_query_response("resp-1")

    assert raw_flags == [True]  # never decoded into a Python tree
    body = orjson.loads(response.body)
    assert body["schema_version"]
    assert body["parameters"] == {"log_level": "DEBUG"}
    assert body["message"] == {"results": []}
    assert body["logs"] == [{"message": "hi", "level": "INFO"}]
    assert list(body)[-1] == "logs"


async def test_an_unknown_response_is_404(redis_mock):
    """#182: a missing id raised KeyError out of the route as a 500."""
    response = await base_routes.get_query_response("no-such-response")
    assert response.status_code == 404


# --- C5-3: finish_query admits by bytes ---------------------------------------


async def test_the_byte_budget_holds_back_work_that_does_not_fit():
    budget = finish._ByteBudget()
    first = await budget.acquire(600, 1000)
    second = asyncio.ensure_future(budget.acquire(600, 1000))
    await asyncio.sleep(0.05)
    assert not second.done()  # 1200 bytes would not fit in 1000

    await budget.release(first)
    assert await asyncio.wait_for(second, 1) == 600


async def test_a_response_larger_than_the_budget_still_runs_alone():
    budget = finish._ByteBudget()
    assert await asyncio.wait_for(budget.acquire(5000, 1000), 1) == 5000
    blocked = asyncio.ensure_future(budget.acquire(10, 1000))
    await asyncio.sleep(0.05)
    assert not blocked.done()
    await budget.release(5000)
    assert await asyncio.wait_for(blocked, 1) == 10


async def test_finish_query_holds_its_response_bytes_while_delivering(
    redis_mock, monkeypatch, mocker
):
    """The delivery is admitted with the response's uncompressed size, and the
    bytes are released once it is sent."""
    await db.save_response("rid", {"message": {"results": []}}, logger)
    state = [None] * 9
    state[8] = "http://callback/1"
    monkeypatch.setattr(finish, "_wait_for_query_state", AsyncMock(return_value=state))
    monkeypatch.setattr(finish, "_too_large_reason", AsyncMock(return_value=None))
    monkeypatch.setattr(finish, "set_query_completed", AsyncMock())
    monkeypatch.setattr(finish, "cleanup_callbacks", AsyncMock())
    monkeypatch.setattr(finish, "get_response_size", AsyncMock(return_value=4321))
    held = []

    async def _send(url, payload, _logger, has_logs=True):
        held.append(finish._inflight.used)
        return True

    monkeypatch.setattr(finish, "send_callback", _send)
    monkeypatch.setattr(finish, "_inflight", finish._ByteBudget())

    task = ("1-0", {"query_id": "qid", "response_id": "rid", "status": "OK"})
    await finish.finish_query(task, logger)

    assert held == [4321]
    assert finish._inflight.used == 0


async def test_deliveries_share_one_http_client(mocker):
    made = []
    real_init = finish.httpx.AsyncClient.__init__

    def _init(self, *args, **kwargs):
        made.append(self)
        real_init(self, *args, **kwargs)

    mocker.patch.object(finish.httpx.AsyncClient, "__init__", _init)
    response = mocker.MagicMock()
    mocker.patch.object(
        finish.httpx.AsyncClient, "post", AsyncMock(return_value=response)
    )

    for _ in range(3):
        assert await finish.send_callback("http://callback/1", b"{}", logger)

    assert len(made) == 1


# --- C8-12: premerge intake decodes in the pool child -------------------------


async def test_intake_does_not_decode_the_response_in_the_worker(
    redis_mock, monkeypatch, mocker
):
    """The response is decoded, counted and stored by the pool child; the
    worker process only ever sees the counts."""
    import shepherd_utils.ars.db as ars_db
    from workers.ars_premerge import worker as pm

    child = {
        "id": "child-1",
        "status": "R",
        "agent": "ara-shepherd-aragorn",
        "ref": "parent-1",
        "result_count": None,
        "params": {},
    }
    parent = {"id": "parent-1", "status": "R", "agent": "ars-default-agent"}
    rows = {"child-1": child, "parent-1": parent}
    mocker.patch.object(
        ars_db, "get_message_row", AsyncMock(side_effect=lambda pk: rows.get(pk))
    )
    update = mocker.patch.object(
        ars_db, "update_message", AsyncMock(side_effect=lambda pk, **kw: kw)
    )
    mocker.patch.object(ars_db, "persist_data_copy", AsyncMock())
    mocker.patch.object(pm, "notify_subscribers", AsyncMock())
    mocker.patch.object(pm.lifecycle, "check_parent_completion", AsyncMock())
    mocker.patch.object(pm, "get_logs", AsyncMock(return_value=[]))
    # Anything decoding the response in the worker process fails the test.
    mocker.patch.object(
        pm, "get_message", AsyncMock(side_effect=AssertionError), create=True
    )
    mocker.patch.object(
        db, "get_message", AsyncMock(side_effect=AssertionError("decoded"))
    )
    stored = {}
    mocker.patch.object(
        pm,
        "get_message_sync",
        side_effect=lambda pk: {"message": {"results": []}, "logs": [1]},
    )
    mocker.patch.object(
        pm, "save_message_sync", side_effect=lambda pk, data: stored.update({pk: data})
    )

    fields = {"intake_child_pk": "child-1", "response_id": "resp-x"}
    assert await pm.intake_internal_response(fields, logger) is None

    final = update.await_args_list[-1].kwargs
    assert (final["status"], final["code"]) == ("D", 200)
    # stored under the child, with the (empty) query logs spliced in
    assert stored == {"child-1": {"message": {"results": []}}}


# --- C2-4: size settings --------------------------------------------------------


def test_size_caps_default_to_what_deployments_run():
    assert settings.max_response_size_bytes == 250 * 10**6
    assert settings.callback_max_request_size_bytes == 250 * 10**6


def test_an_unreadable_size_setting_is_warned_about(caplog):
    config._warned_sizes.discard("300 MBs")
    with caplog.at_level(logging.WARNING, logger="shepherd.config"):
        assert config.parse_size_to_bytes("300 MBs") == 0
        assert config.parse_size_to_bytes("300 MBs") == 0
    assert caplog.text.count("'300 MBs'") == 1
