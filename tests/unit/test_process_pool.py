"""Tests for ProcessPoolManager's broken-pool handling.

When a pool child dies (e.g. an OOM kill), the manager must NOT rebuild the pool
in-process -- doing that on the event loop thread can deadlock the worker. It
instead fails the task and signals a clean restart exactly once via the
``on_broken`` callback, which the workers wire to the graceful-drain shutdown.
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


async def test_broken_pool_signals_restart_and_does_not_rebuild():
    loop = asyncio.get_running_loop()
    calls = []
    pool = ProcessPoolManager(
        max_workers=1, name="test pool", on_broken=lambda: calls.append(1)
    )
    try:
        # Healthy pool runs a task fine.
        assert await pool.run(loop, _echo, "before") == "before"

        executor_before = pool._executor

        # A child dying surfaces as BrokenProcessPool for the triggering task.
        with pytest.raises(BrokenProcessPool):
            await pool.run(loop, _suicide, None)

        # The pool is NOT rebuilt in-process (that risks a loop deadlock)...
        assert pool._executor is executor_before
        assert pool.is_broken is True
        # ...instead a restart is requested exactly once.
        assert calls == [1]
    finally:
        pool.shutdown()


async def test_broken_pool_signals_only_once_under_concurrent_failures():
    """Every in-flight future on the dead pool raises; on_broken fires once."""
    loop = asyncio.get_running_loop()
    calls = []
    pool = ProcessPoolManager(
        max_workers=2, name="test pool", on_broken=lambda: calls.append(1)
    )
    try:
        await pool.run(loop, _echo, "warmup")

        results = await asyncio.gather(
            pool.run(loop, _suicide, None),
            pool.run(loop, _suicide, None),
            return_exceptions=True,
        )
        assert all(isinstance(r, BrokenProcessPool) for r in results)
        assert calls == [1]
    finally:
        pool.shutdown()


async def test_broken_pool_without_callback_just_marks_broken():
    """on_broken=None is tolerated (logs only); the flag still flips."""
    loop = asyncio.get_running_loop()
    pool = ProcessPoolManager(max_workers=1, name="test pool")
    try:
        with pytest.raises(BrokenProcessPool):
            await pool.run(loop, _suicide, None)
        assert pool.is_broken is True
    finally:
        pool.shutdown()
