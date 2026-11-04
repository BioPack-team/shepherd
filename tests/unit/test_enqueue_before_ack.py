"""Hand-offs between pipeline steps must not drop a query (#202).

``add_task`` used to swallow enqueue failures everywhere, and several
hand-offs ACKed their own task (or answered the caller) regardless. One broker
blip at the wrong moment then left a query with no next step and nobody told:
it sat until the abandoned-query reaper, and its callback never fired. Each
test here makes every ``XADD`` fail and checks the hand-off holds on to the
work (or tells the caller) instead.
"""

import logging
from unittest.mock import AsyncMock

import pytest
from starlette.requests import Request

from shepherd_utils import broker, shared

logger = logging.getLogger(__name__)


def _task_fields(**extra):
    return {
        "query_id": "q1",
        "response_id": "r1",
        "workflow": '[{"id": "sort_results_score"}]',
        "log_level": "20",
        "otel": "{}",
        "metadata": "{}",
        **extra,
    }


@pytest.fixture
def broker_down(redis_mock, monkeypatch):
    """Every XADD fails, as during a broker blip; everything else works."""
    calls = []

    async def _xadd(*args, **kwargs):
        calls.append(args)
        raise ConnectionError("broker unavailable")

    monkeypatch.setattr(redis_mock["broker"], "xadd", _xadd)
    monkeypatch.setattr(broker.asyncio, "sleep", AsyncMock())
    return calls


@pytest.fixture
def acks(monkeypatch):
    """Record ACKs instead of touching the stream; stub the log/metric flushes."""
    acked = []

    async def _mark(stream, group, msg_id, _logger, retries=0):
        acked.append(msg_id)

    monkeypatch.setattr(shared, "mark_task_as_complete", _mark)
    monkeypatch.setattr(shared, "save_logs", AsyncMock())
    monkeypatch.setattr(shared, "_record_task_duration", AsyncMock())
    return acked


async def test_strict_add_task_retries_then_raises(broker_down):
    with pytest.raises(ConnectionError):
        await broker.add_task("q", {"a": "b"}, logger, raise_on_failure=True)
    assert len(broker_down) == broker.STRICT_ADD_ATTEMPTS


async def test_default_add_task_still_swallows_after_one_try(broker_down):
    await broker.add_task("q", {"a": "b"}, logger)
    assert len(broker_down) == 1


async def test_wrap_up_does_not_ack_when_the_next_step_cannot_be_enqueued(
    broker_down, acks
):
    """C1-1: the current task stays un-ACKed and the error surfaces."""
    task = ("1-0", _task_fields())
    with pytest.raises(ConnectionError):
        await shared.wrap_up_task("sort_results_score", "consumer", task, logger)
    assert acks == []


async def test_wrap_up_acks_after_a_successful_hand_off(redis_mock, acks):
    task = ("1-0", _task_fields())
    await shared.wrap_up_task("sort_results_score", "consumer", task, logger)
    assert acks == ["1-0"]
    entries = await redis_mock["broker"].xrange("finish_query")
    assert entries and entries[0][1]["query_id"] == "q1"


async def test_failure_path_does_not_ack_before_finish_query_is_enqueued(
    broker_down, acks
):
    """C1-2: the error wrap-up leaves the task un-ACKed if finish_query can't
    be queued, rather than ACKing first and losing the query."""
    task = ("1-0", _task_fields())
    await shared.handle_task_failure("sort_results_score", "consumer", task, logger)
    assert acks == []


async def test_failure_path_enqueues_then_acks(redis_mock, acks):
    task = ("1-0", _task_fields())
    await shared.handle_task_failure("sort_results_score", "consumer", task, logger)
    assert acks == ["1-0"]
    entries = await redis_mock["broker"].xrange("finish_query")
    assert entries[0][1]["status"].startswith("ERROR")


async def test_intake_reports_failure_when_the_query_cannot_be_enqueued(
    broker_down, monkeypatch
):
    """C3-2: run_query raises QueryIntakeError (503) instead of returning ids
    for a query no worker will ever pick up."""
    from shepherd_server import base_routes

    monkeypatch.setattr(base_routes, "add_query", AsyncMock())
    query = {"message": {"query_graph": {"nodes": {"n0": {}}}}}
    with pytest.raises(base_routes.QueryIntakeError):
        await base_routes.run_query("aragorn", query, "http://callback/1")


def _callback_request(body: bytes) -> Request:
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


@pytest.fixture
def callback_route(monkeypatch):
    from shepherd_server import base_routes

    async def _query_id(callback_id, _logger):
        return ("q-1", "{}")

    async def _state(query_id, _logger):
        return [None] * 7 + ["resp-1"]

    monkeypatch.setattr(base_routes, "get_callback_query_id", _query_id)
    monkeypatch.setattr(base_routes, "get_query_state", _state)
    monkeypatch.setattr(base_routes, "add_ready_callback", AsyncMock())
    monkeypatch.setattr(base_routes, "save_logs", AsyncMock())
    return base_routes


async def test_callback_asks_for_redelivery_when_its_merge_cannot_be_queued(
    broker_down, callback_route
):
    """C3-4: a callback whose merge wake task can't be queued gets a 500, so
    the sender redelivers, instead of a 200 for results nothing will merge."""
    response = await callback_route.callback(
        callback_route.ARATargetEnum.ARAGORN,
        "cb-1",
        _callback_request(b'{"message": {"results": []}}'),
    )
    assert response.status_code == 500


async def test_callback_is_acknowledged_once_its_merge_is_queued(
    redis_mock, callback_route
):
    response = await callback_route.callback(
        callback_route.ARATargetEnum.ARAGORN,
        "cb-1",
        _callback_request(b'{"message": {"results": []}}'),
    )
    assert response.status_code == 200
    entries = await redis_mock["broker"].xrange("merge_message")
    assert entries[0][1]["callback_id"] == "cb-1"
