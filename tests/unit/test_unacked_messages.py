"""Messages left pending under a live consumer are never stranded (#213).

``reclaim_orphaned`` skips messages owned by a live consumer, itself included,
so any path that neither acked nor failed a message left it pending for good:
the query sat until the abandoned-query reaper, with no callback.

* C1-3: ``run_task_lifecycle`` logged a wrap-up failure and swallowed
  ``CancelledError``, acking nothing and failing nothing.
* C4-3: a merge_message error after taking the query's lock but before the
  drain left the lock held and re-enqueued nothing.
* C1-9: reclaim only read the first ``reclaim_max_batch`` idle PEL entries.
"""

import asyncio
import json
import logging
import time
from unittest.mock import AsyncMock

import pytest

from shepherd_utils import db, reclaim, shared
from shepherd_utils.config import settings

logger = logging.getLogger(__name__)

STREAM, GROUP = "aragorn.score", "consumer"


class _Limiter:
    def __init__(self):
        self.released = 0

    def release(self):
        self.released += 1


async def _deliver(broker, fields=None, consumer="me", stream=STREAM):
    """Add a message and leave it pending (delivered, un-acked) to ``consumer``."""
    fields = fields or {
        "query_id": "q1",
        "response_id": "r1",
        "workflow": json.dumps([{"id": STREAM}, {"id": "sort_results_score"}]),
        "otel": "{}",
        "metadata": "{}",
    }
    msg_id = await broker.xadd(stream, fields)
    try:
        await broker.xgroup_create(stream, GROUP, "0", mkstream=True)
    except Exception:
        pass  # group already exists
    delivered = await broker.xreadgroup(GROUP, consumer, {stream: ">"}, count=1)
    return delivered[0][1][0]


async def _pending_ids(broker, stream=STREAM):
    entries = await broker.xpending_range(stream, GROUP, min="-", max="+", count=100)
    return [entry["message_id"] for entry in entries]


async def _queued(broker, stream):
    return [fields for _, fields in await broker.xrange(stream)]


# --- C1-3: run_task_lifecycle --------------------------------------------


async def _noop_worker(task, task_logger):
    return None


async def test_a_failed_wrap_up_fails_the_query_instead_of_stranding_it(
    redis_mock, monkeypatch
):
    broker = redis_mock["broker"]
    task = await _deliver(broker)

    async def _broken_wrap_up(*args, **kwargs):
        raise ConnectionError("broker unreachable")

    monkeypatch.setattr(shared, "wrap_up_task", _broken_wrap_up)
    limiter = _Limiter()

    await shared.run_task_lifecycle(
        STREAM, GROUP, task, None, logger, limiter, _noop_worker
    )

    finished = await _queued(broker, "finish_query")
    assert [f["query_id"] for f in finished] == ["q1"]
    assert finished[0]["status"] != "OK"
    assert await _pending_ids(broker) == []
    assert limiter.released == 1


async def test_a_stray_cancellation_fails_the_query_instead_of_stranding_it(
    redis_mock,
):
    """A CancelledError the operation raised (e.g. its pool future was
    cancelled when the executor was replaced), not this task being cancelled."""
    broker = redis_mock["broker"]
    task = await _deliver(broker)

    async def _cancelled_inside(task, task_logger):
        raise asyncio.CancelledError()

    await shared.run_task_lifecycle(
        STREAM, GROUP, task, None, logger, _Limiter(), _cancelled_inside
    )

    finished = await _queued(broker, "finish_query")
    assert [f["query_id"] for f in finished] == ["q1"]
    assert await _pending_ids(broker) == []


async def test_cancelling_the_task_leaves_the_message_for_reclaim(redis_mock):
    """Shutdown cancels the task itself: the message stays pending for a peer
    to reclaim once this consumer is gone, and the cancellation propagates."""
    broker = redis_mock["broker"]
    task = await _deliver(broker)
    started = asyncio.Event()

    async def _long(task, task_logger):
        started.set()
        await asyncio.sleep(60)

    limiter = _Limiter()
    running = asyncio.ensure_future(
        shared.run_task_lifecycle(STREAM, GROUP, task, None, logger, limiter, _long)
    )
    await started.wait()
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running

    assert await _pending_ids(broker) == [task[0]]
    assert await _queued(broker, "finish_query") == []
    assert limiter.released == 1


# --- C1-9 and the backstop: reclaim -----------------------------------------


@pytest.fixture()
def tiny_idle_floor(monkeypatch, redis_mock):
    # fakeredis mishandles IDLE 0; see test_reclaim.py.
    monkeypatch.setattr(reclaim, "min_idle_sec_for", lambda *a, **k: 0.001)
    monkeypatch.setattr(reclaim, "broker_client", redis_mock["broker"])


async def test_reclaim_reaches_orphans_behind_a_full_page_of_live_work(
    redis_mock, tiny_idle_floor, monkeypatch
):
    broker = redis_mock["broker"]
    monkeypatch.setattr(settings, "reclaim_max_batch", 2)
    # Three long-running tasks of a live consumer fill the first page...
    for _ in range(3):
        await _deliver(broker, {"query_id": "busy"}, consumer="busy")
    await broker.set(
        f"worker:heartbeat:{STREAM}:busy", json.dumps({"last_seen": time.time()})
    )
    # ...ahead of a dead consumer's orphan.
    orphan = await _deliver(broker, {"query_id": "orphan"}, consumer="dead")
    await asyncio.sleep(0.02)

    reclaimed = await reclaim.reclaim_orphaned(STREAM, GROUP, "me", logger)

    assert [m[0] for m in reclaimed] == [orphan[0]]


async def test_reclaim_takes_back_own_messages_that_are_no_longer_running(
    redis_mock, tiny_idle_floor
):
    broker = redis_mock["broker"]
    running = await _deliver(broker, {"query_id": "running"})
    stranded = await _deliver(broker, {"query_id": "stranded"})
    await asyncio.sleep(0.02)
    counts = {}

    reclaimed = await reclaim.reclaim_orphaned(
        STREAM,
        GROUP,
        "me",
        logger,
        delivery_counts=counts,
        in_flight_ids={running[0]},
    )

    assert [m[0] for m in reclaimed] == [stranded[0]]
    # Counted like any redelivery, so the poison-pill breaker still applies.
    assert counts == {stranded[0]: 1}


async def test_reclaim_leaves_own_messages_alone_without_in_flight_ids(
    redis_mock, tiny_idle_floor
):
    """Callers that don't track what they run keep the old behaviour."""
    broker = redis_mock["broker"]
    await _deliver(broker)
    await asyncio.sleep(0.02)

    assert await reclaim.reclaim_orphaned(STREAM, GROUP, "me", logger) == []


async def test_task_slots_track_running_message_ids():
    slots = shared.TaskSlots(2)
    await slots.acquire()
    first = slots.dispatch("1-0")
    await slots.acquire()
    second = slots.dispatch("2-0")
    assert slots.in_flight_ids == {"1-0", "2-0"}
    assert slots.in_flight == 2

    first.release()
    first.release()  # a second release frees nothing more
    assert slots.in_flight_ids == {"2-0"}
    assert slots.in_flight == 1
    second.release()
    assert slots.in_flight_ids == set()
    assert slots.in_flight == 0


async def test_get_tasks_hands_out_a_permit_and_reports_running_ids(monkeypatch):
    seen_in_flight = []

    async def _reclaim(*args, in_flight_ids=None, **kwargs):
        seen_in_flight.append(in_flight_ids)
        return []

    class _HB:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            return self

        async def mark_clean_shutdown(self):
            pass

        async def stop(self):
            pass

    task = ("7-0", {"query_id": "q1", "response_id": "r1", "otel": "{}"})

    async def _get_task(*args, **kwargs):
        shared._request_shutdown()
        return task

    async def _noop(*args, **kwargs):
        return None

    async def _not_expired(*args, **kwargs):
        return False

    monkeypatch.setattr(shared, "_shutdown", asyncio.Event())
    monkeypatch.setattr(shared, "_active_heartbeat", None)
    monkeypatch.setattr(shared, "initialize_db", _noop)
    monkeypatch.setattr(shared, "Heartbeat", _HB)
    monkeypatch.setattr(shared, "install_shutdown_handlers", lambda hb=None: None)
    monkeypatch.setattr(shared, "reclaim_orphaned", _reclaim)
    monkeypatch.setattr(shared, "get_task", _get_task)
    monkeypatch.setattr(shared, "_handled_as_expired", _not_expired)
    monkeypatch.setattr(settings, "broker_unhealthy_exit_sec", 0.0)

    yielded = []
    with pytest.raises(SystemExit):
        async for got, _ctx, _logger, permit in shared.get_tasks(
            STREAM, GROUP, "me", 4
        ):
            yielded.append(got[0])
            assert permit.msg_id == "7-0"
            permit.release()

    assert yielded == ["7-0"]
    assert seen_in_flight == [set()]


# --- C4-3: merge_message setup errors ---------------------------------------


async def test_a_merge_setup_error_releases_the_lock_and_retries(
    redis_mock, monkeypatch
):
    from workers.merge_message import worker as merge

    response_id = "resp-setup"
    await redis_mock["data"].set("q-setup", b"x")
    await redis_mock["data"].set(response_id, b"x")
    await db.add_ready_callback(response_id, "cb-1", logger)

    lock = redis_mock["lock"]

    async def _remove_lock(key, consumer, _logger):
        if await lock.get(key) == consumer:
            await lock.delete(key)

    async def _refresh_lock(key, consumer, ttl_ms, _logger):
        return await lock.get(key) == consumer

    async def _blip(response_id):
        raise ConnectionError("redis blip")

    monkeypatch.setattr(merge, "remove_lock", _remove_lock)
    monkeypatch.setattr(merge, "refresh_lock", _refresh_lock)
    monkeypatch.setattr(merge, "get_merge_crashes", _blip)
    monkeypatch.setattr(merge, "resolve_pool_workers", lambda limit, lg: 1)
    monkeypatch.setattr(settings, "merge_retry_backoff", 0)
    reenqueued = AsyncMock()
    monkeypatch.setattr(merge, "_reenqueue_wake_task", reenqueued)
    acked = asyncio.get_running_loop().create_future()

    async def _ack(stream, group, msg_id, _logger, *args):
        if not acked.done():
            acked.set_result(msg_id)

    monkeypatch.setattr(merge, "mark_task_as_complete", _ack)

    class _Pool:
        def __init__(self, *args, **kwargs):
            pass

        def shutdown(self):
            pass

    async def _one_task(stream, group, consumer, limit):
        yield (
            "1-0",
            {
                "query_id": "q-setup",
                "response_id": response_id,
                "callback_id": "cb-1",
                "target": "aragorn",
                "log_level": "20",
                "otel": "{}",
                "metadata": "{}",
            },
        ), None, logging.getLogger("shepherd.test.setup"), _Limiter()
        await asyncio.sleep(3600)

    # the shared poll loop (run_poll_loop) reads it from shared
    monkeypatch.setattr(shared, "get_tasks", _one_task)
    monkeypatch.setattr(merge, "ProcessPoolManager", _Pool)
    poller = asyncio.create_task(merge.poll_for_tasks())
    try:
        await asyncio.wait_for(acked, timeout=10)
    finally:
        poller.cancel()
        try:
            await poller
        except BaseException:
            pass

    assert await lock.get(response_id) is None
    reenqueued.assert_awaited_once()
