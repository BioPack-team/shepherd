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

        first = pool._executor

        # A child dying surfaces as BrokenProcessPool for the triggering task...
        with pytest.raises(BrokenProcessPool):
            await pool.run(loop, _suicide, None)

        # ...but the manager swaps in a fresh executor...
        assert pool._executor is not first

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
        first = pool._executor

        # Child sleeps well past the 1s limit -> the run must time out.
        with pytest.raises(asyncio.TimeoutError):
            await pool.run(loop, _sleep, 30)

        # The stuck child was killed and the pool replaced...
        assert pool._executor is not first

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
        broken = pool._executor

        # Two callers both observed `broken`; the first swaps it, the second is
        # a no-op (identity guard) rather than churning a second fresh pool.
        await pool._replace(broken)
        replaced_once = pool._executor
        await pool._replace(broken)

        assert replaced_once is not broken
        assert pool._executor is replaced_once
    finally:
        pool.shutdown()
