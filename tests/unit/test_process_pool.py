"""Tests for the self-healing ProcessPoolManager.

Regression coverage for the wedged-worker bug: a single OOM-killed child used to
poison the shared ProcessPoolExecutor so every subsequent task failed with "A
process in the process pool was terminated abruptly...". The manager must replace
the broken pool so the very next task succeeds -- and it must use the spawn start
method, because rebuilding a fork-context pool on the event loop thread can
deadlock the worker.
"""

import asyncio
import os
import time

from concurrent.futures.process import BrokenProcessPool

import pytest

from shepherd_utils.process_pool import ProcessPoolManager


# Top-level (picklable) callables so the process pool can dispatch them.
def _echo(value):
    return value


def _suicide(_):
    # Abruptly kill the child so the executor enters its broken state, mimicking
    # a kernel OOM kill of a worker processing an oversized message.
    os._exit(1)


def _get_start_method(_):
    # Runs in a child; reports the start method the child was launched with.
    import multiprocessing

    return multiprocessing.get_start_method()


def _sleep(seconds):
    # Runs in a child; blocks so we can exercise the task timeout.
    import time

    time.sleep(seconds)
    return "done"


async def test_pool_uses_spawn_start_method():
    """Children must be spawned, not forked (fork risks a loop-thread deadlock)."""
    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=1, name="test pool")
    try:
        assert await pool.run(loop, _get_start_method, None) == "spawn"
    finally:
        pool.shutdown()


async def test_manager_recovers_after_child_dies():
    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=1, name="test pool")
    try:
        # Healthy pool runs a task fine.
        assert await pool.run(loop, _echo, "before") == "before"

        first = pool._slots[0]

        # A child dying surfaces as BrokenProcessPool for the triggering task...
        with pytest.raises(BrokenProcessPool):
            await pool.run(loop, _suicide, None)

        # ...but the manager swaps in a fresh executor...
        assert pool._slots[0] is not first

        # ...so the next task runs on the healthy pool instead of re-raising.
        assert await pool.run(loop, _echo, "after") == "after"
    finally:
        pool.shutdown()


async def test_run_times_out_kills_child_and_recovers():
    """A task that overruns task_timeout is killed and the pool rebuilt.

    Without this a hung child would hold its pool slot forever and, with a small
    pool, silently stall the whole worker. The task must fail (TimeoutError) and
    the very next task must succeed on a fresh pool.
    """
    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=1, name="test pool", task_timeout=1.0)
    try:
        first = pool._slots[0]

        # Child sleeps well past the 1s limit -> the run must time out.
        with pytest.raises(asyncio.TimeoutError):
            await pool.run(loop, _sleep, 30)

        # The stuck child was killed and its slot replaced...
        assert pool._slots[0] is not first

        # ...so the next task runs on a healthy pool.
        assert await pool.run(loop, _echo, "after") == "after"
    finally:
        pool.shutdown()


async def test_no_timeout_when_task_timeout_unset():
    """With task_timeout None, a fast task still completes normally."""
    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=1, name="test pool")
    try:
        assert await pool.run(loop, _echo, "ok") == "ok"
    finally:
        pool.shutdown()


async def test_replace_is_idempotent_for_the_same_broken_pool():
    """Concurrent tasks that all saw the same dead pool replace it only once."""
    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=2, name="test pool")
    try:
        await pool.run(loop, _echo, "warmup")
        broken = pool._slots[0]

        # Two callers both observed `broken`; the first swaps it, the second is
        # a no-op (identity guard) rather than churning a second fresh pool.
        await pool._replace(0, broken)
        replaced_once = pool._slots[0]
        await pool._replace(0, broken)

        assert replaced_once is not broken
        assert pool._slots[0] is replaced_once
    finally:
        pool.shutdown()


def _noop():
    return None


def _children(pool):
    return sum(len(executor._processes) for executor in pool._slots)


async def test_warmup_spawns_every_child_up_front():
    """With a warmup hook every child is spawned when the pool is built (and
    rebuilt), not lazily on the first real tasks."""
    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=2, name="test pool", warmup=_noop)
    try:
        assert _children(pool) == 2
        first = list(pool._slots)
        with pytest.raises(BrokenProcessPool):
            await pool.run(loop, _suicide, None)
        assert pool._slots != first
        assert _children(pool) == 2
        assert await pool.run(loop, _echo, "ok") == "ok"
    finally:
        pool.shutdown()


async def test_no_warmup_keeps_children_lazy():
    pool = ProcessPoolManager(max_workers=2, name="test pool")
    try:
        assert _children(pool) == 0
    finally:
        pool.shutdown()


# --- one task's death or timeout costs only that task (#212) --------------


async def _until_spawned(pool, count, timeout=20.0):
    """Wait until ``count`` warmed-up children have finished starting."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        started = [
            executor
            for executor in pool._slots
            if executor._processes and executor._idle_worker_semaphore._value > 0
        ]
        if len(started) >= count:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("pool children did not start")


def _slow_echo(value):
    import time

    time.sleep(1.5)
    return value


def _die_soon(_):
    import time

    time.sleep(0.3)
    os._exit(1)


async def test_a_child_dying_does_not_fail_the_task_beside_it():
    """An OOM kill of one task's child used to break the shared executor and
    fail every task in flight with it."""
    loop = asyncio.get_running_loop()
    # Warmed up so both children exist before either task starts: spawned on
    # demand, the second task can queue behind the first in a single child,
    # and the two never overlap.
    pool = ProcessPoolManager(max_workers=2, name="test pool", warmup=_noop)
    try:
        await _until_spawned(pool, 2)
        bystander = asyncio.ensure_future(pool.run(loop, _slow_echo, "survived"))
        await asyncio.sleep(0.1)
        with pytest.raises(BrokenProcessPool):
            await pool.run(loop, _die_soon, None)
        assert await bystander == "survived"
    finally:
        pool.shutdown()


async def test_a_timeout_does_not_kill_the_task_beside_it():
    """A timed-out task's child is killed; the other running task is not."""
    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(
        max_workers=2, name="test pool", task_timeout=1.0, warmup=_noop
    )
    try:
        await _until_spawned(pool, 2)
        stuck = asyncio.ensure_future(pool.run(loop, _sleep, 30))
        await asyncio.sleep(0.6)
        bystander = asyncio.ensure_future(pool.run(loop, _echo, "survived"))
        with pytest.raises(asyncio.TimeoutError):
            await stuck
        assert await bystander == "survived"
    finally:
        pool.shutdown()


async def test_the_timeout_starts_when_the_task_gets_a_slot():
    """Waiting for a busy slot doesn't count against a task's timeout."""
    loop = asyncio.get_running_loop()
    # Warmed up and waited for: spawning the child on the first task's clock
    # could push it past 2s on a loaded CI runner, which isn't what this test
    # is about.
    pool = ProcessPoolManager(
        max_workers=1, name="test pool", task_timeout=2.0, warmup=_noop
    )
    try:
        await _until_spawned(pool, 1)
        first = asyncio.ensure_future(pool.run(loop, _slow_echo, "first"))
        await asyncio.sleep(0.1)
        second = asyncio.ensure_future(pool.run(loop, _slow_echo, "second"))
        assert await first == "first"
        assert await second == "second"
    finally:
        pool.shutdown()


async def test_a_per_call_timeout_tightens_the_pools():
    """run(..., timeout=) cuts this call short of the pool's own ceiling."""
    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=1, name="test pool", task_timeout=60.0)
    try:
        started = loop.time()
        with pytest.raises(asyncio.TimeoutError):
            await pool.run(loop, _sleep, 30, timeout=1.0)
        assert loop.time() - started < 20
        assert await pool.run(loop, _echo, "after") == "after"
    finally:
        pool.shutdown()


# --- run_within_budget (#245) ------------------------------------------------


def _fields(seconds_left):
    from shepherd_utils.task_deadline import DEADLINE_FIELD

    return {DEADLINE_FIELD: f"{time.time() + seconds_left:.3f}"}


async def test_a_query_budget_shorter_than_the_pool_timeout_bounds_the_task():
    """The task is cut off when its query's budget runs out and the query is
    told so (TIMEOUT, naming the step), not held for the pool's full ceiling."""
    from shepherd_utils.query_errors import QueryTimedOut

    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=1, name="test pool", task_timeout=60.0)
    try:
        started = loop.time()
        with pytest.raises(QueryTimedOut) as raised:
            await pool.run_within_budget(loop, _fields(1.0), "Test step", _sleep, 30)
        assert loop.time() - started < 20
        assert raised.value.query_status == "TIMEOUT"
        assert raised.value.description.startswith("Test step")
        # the slot was rebuilt and works
        assert await pool.run(loop, _echo, "after") == "after"
    finally:
        pool.shutdown()


async def test_the_pool_timeout_still_applies_when_the_budget_is_longer():
    """A task slower than the pool's ceiling while its query still has time is
    the task's failure, not the query's TIMEOUT."""
    from shepherd_utils.query_errors import QueryTimedOut

    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=1, name="test pool", task_timeout=1.0)
    try:
        with pytest.raises(asyncio.TimeoutError) as raised:
            await pool.run_within_budget(loop, _fields(3600), "Test step", _sleep, 30)
        assert not isinstance(raised.value, QueryTimedOut)
    finally:
        pool.shutdown()


async def test_a_query_already_out_of_budget_is_not_started(mocker):
    from shepherd_utils.query_errors import QueryTimedOut

    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=1, name="test pool", task_timeout=60.0)
    run = mocker.spy(pool, "run")
    try:
        with pytest.raises(QueryTimedOut):
            await pool.run_within_budget(loop, _fields(-1), "Test step", _echo, "x")
        run.assert_not_called()
    finally:
        pool.shutdown()


async def test_a_task_without_a_deadline_just_runs():
    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=1, name="test pool", task_timeout=60.0)
    try:
        assert await pool.run_within_budget(loop, {}, "Test step", _echo, "x") == "x"
    finally:
        pool.shutdown()
