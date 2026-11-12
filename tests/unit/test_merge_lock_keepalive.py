"""The merge locks must outlive the work done under them (#204).

merge_message took a 45s lock and refreshed it only between passes, but one
pass can run for minutes. A pass that outlived the lock let a second worker
start merging the same response from the same stored accumulator; whichever
save landed last won and the other batch's callbacks were lost. ars_merge
already kept its lock alive, but carried on folding after finding it lost.
"""

import asyncio
import contextlib
import logging
from unittest.mock import AsyncMock

import pytest

from shepherd_utils import shared

from shepherd_utils import db

logger = logging.getLogger(__name__)


class _Limiter:
    def release(self):
        pass


async def _run_one_merge_task(merge, response_id, callback_id, pool_cls, done):
    """Drive merge_message's poll loop over a single wake task until ``done``."""

    async def _one_task(stream, group, consumer, limit):
        task = (
            "1-0",
            {
                "query_id": "q-lock",
                "response_id": response_id,
                "callback_id": callback_id,
                "target": "aragorn",
                "log_level": "20",
                "otel": "{}",
                "metadata": "{}",
            },
        )
        yield task, None, logging.getLogger("shepherd.test.lock"), _Limiter()
        await asyncio.sleep(3600)

    # the shared poll loop (run_poll_loop) reads it from shared
    shared.get_tasks = _one_task
    merge.ProcessPoolManager = pool_cls
    poller = asyncio.create_task(merge.poll_for_tasks())
    try:
        return await asyncio.wait_for(done, timeout=10)
    finally:
        poller.cancel()
        try:
            await poller
        except BaseException:
            pass


@pytest.fixture
def merge_env(redis_mock, monkeypatch):
    from workers.merge_message import worker as merge

    async def _setup(response_id, callback_ids):
        await redis_mock["data"].set("q-lock", b"x")
        await redis_mock["data"].set(response_id, b"x")
        for cb in callback_ids:
            await db.add_ready_callback(response_id, cb, logger)
        monkeypatch.setattr(
            merge,
            "get_existing_callback_ids",
            AsyncMock(return_value=set(callback_ids)),
        )

    # The real refresh/remove are Lua scripts, which fakeredis can't run
    # without the optional lupa package; these do the same compare-and-act.
    lock = redis_mock["lock"]

    async def _refresh_lock(key, consumer, ttl_ms, _logger):
        if await lock.get(key) != consumer:
            return False
        return bool(await lock.pexpire(key, ttl_ms))

    async def _remove_lock(key, consumer, _logger):
        if await lock.get(key) == consumer:
            await lock.delete(key)

    monkeypatch.setattr(merge, "refresh_lock", _refresh_lock)
    monkeypatch.setattr(merge, "remove_lock", _remove_lock)
    monkeypatch.setattr(merge, "remove_callback_id", AsyncMock())
    monkeypatch.setattr(merge, "resolve_pool_workers", lambda limit, lg: 1)
    monkeypatch.setattr(shared, "get_tasks", shared.get_tasks)
    monkeypatch.setattr(merge, "ProcessPoolManager", merge.ProcessPoolManager)
    return merge, _setup


async def test_merge_lock_is_still_held_at_the_end_of_a_long_pass(
    redis_mock, merge_env, monkeypatch
):
    """C4-1: with a 1s TTL and a 2.5s pass, the lock is still ours at the end
    of the pass, as a racing worker would see it."""
    merge, setup = merge_env
    response_id, callback_id = "resp-lock", "cb-lock"
    await setup(response_id, [callback_id])
    monkeypatch.setattr(merge, "LOCK_TTL_SEC", 1)
    observed = asyncio.get_running_loop().create_future()

    class _SlowPool:
        def __init__(self, *args, **kwargs):
            pass

        @contextlib.asynccontextmanager
        async def slot(self):
            yield self

        async def run(self, loop, fn, target, query_id, rid, ready, level):
            await asyncio.sleep(2.5)
            if not observed.done():
                observed.set_result(await redis_mock["lock"].get(response_id))
            return list(ready), []

        def shutdown(self):
            pass

    holder = await _run_one_merge_task(
        merge, response_id, callback_id, _SlowPool, observed
    )
    assert holder == merge.CONSUMER


async def test_merge_lock_is_released_when_the_drain_finishes(
    redis_mock, merge_env, monkeypatch
):
    merge, setup = merge_env
    response_id, callback_id = "resp-free", "cb-free"
    await setup(response_id, [callback_id])
    monkeypatch.setattr(merge, "LOCK_TTL_SEC", 1)
    finished = asyncio.get_running_loop().create_future()
    real_save_logs = merge.save_logs

    async def _save_logs(rid, lg):
        await real_save_logs(rid, lg)
        if not finished.done():
            finished.set_result(None)

    monkeypatch.setattr(merge, "save_logs", _save_logs)

    class _QuickPool:
        def __init__(self, *args, **kwargs):
            pass

        @contextlib.asynccontextmanager
        async def slot(self):
            yield self

        async def run(self, loop, fn, target, query_id, rid, ready, level):
            return list(ready), []

        def shutdown(self):
            pass

    await _run_one_merge_task(merge, response_id, callback_id, _QuickPool, finished)
    # released, and not resurrected by a keep-alive left running
    await asyncio.sleep(0.5)
    assert await redis_mock["lock"].get(response_id) is None


async def test_drain_stops_once_the_lock_is_lost(redis_mock, merge_env, monkeypatch):
    """If a refresh finds the lock gone, no further pass is started."""
    merge, setup = merge_env
    response_id = "resp-lost"
    callbacks = ["cb-a", "cb-b"]
    await setup(response_id, callbacks)
    monkeypatch.setattr(merge, "LOCK_TTL_SEC", 1)
    monkeypatch.setattr(merge.settings, "merge_max_fold", 1)
    monkeypatch.setattr(merge, "refresh_lock", AsyncMock(return_value=False))
    passes = []
    finished = asyncio.get_running_loop().create_future()
    real_save_logs = merge.save_logs

    async def _save_logs(rid, lg):
        await real_save_logs(rid, lg)
        if not finished.done():
            finished.set_result(None)

    monkeypatch.setattr(merge, "save_logs", _save_logs)
    monkeypatch.setattr(merge, "_reenqueue_wake_task", AsyncMock())

    class _Pool:
        def __init__(self, *args, **kwargs):
            pass

        @contextlib.asynccontextmanager
        async def slot(self):
            yield self

        async def run(self, loop, fn, target, query_id, rid, ready, level):
            passes.append(list(ready))
            await asyncio.sleep(0.6)
            return list(ready), []

        def shutdown(self):
            pass

    await _run_one_merge_task(merge, response_id, "cb-a", _Pool, finished)
    assert len(passes) == 1


async def test_ars_merge_stops_folding_once_its_lock_is_lost(mocker):
    """C8-13: ars_merge's keep-alive finding the lock gone stops the drain
    before the next child is folded."""
    from workers.ars_merge import worker as ars_merge

    mocker.patch.object(ars_merge, "LOCK_REFRESH_SEC", 0.05)
    mocker.patch.object(ars_merge, "try_lock", AsyncMock(return_value=True))
    mocker.patch.object(ars_merge, "refresh_lock", AsyncMock(return_value=False))
    mocker.patch.object(ars_merge, "remove_lock", AsyncMock())
    mocker.patch.object(ars_merge, "add_task", AsyncMock())
    mocker.patch.object(
        ars_merge.ars_db,
        "get_ready_children",
        AsyncMock(side_effect=[["c1", "c2"], ["c2"], []]),
    )
    mocker.patch.object(ars_merge.ars_db, "clear_ready_child", AsyncMock())

    async def _slow_merge(parent_pk, child_pk, lg):
        await asyncio.sleep(0.2)

    merge_one = mocker.patch.object(
        ars_merge, "merge_one", AsyncMock(side_effect=_slow_merge)
    )

    await ars_merge.ars_merge(("1-0", {"parent_pk": "p1"}), logger)

    assert merge_one.await_count == 1
