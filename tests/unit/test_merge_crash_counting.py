"""A merge pass a rollout cuts short is not a crash (#212, C4-7).

merge_message counts a pass as a crash before it starts and uncounts it when
it returns, so a pass killed by a normal rollout (SIGTERM, then the drain
timeout) stayed counted; three rollouts during a long merge failed a healthy
query as RESPONSE_TOO_LARGE.
"""

import logging
from unittest.mock import AsyncMock

import pytest

from shepherd_utils import db, shared

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def _clean_hooks():
    saved = list(shared._shutdown_hooks)
    yield
    shared._shutdown_hooks[:] = saved


async def test_graceful_drain_runs_the_shutdown_hooks(monkeypatch):
    hook = AsyncMock()
    shared.on_graceful_shutdown(hook)
    monkeypatch.setattr(shared.settings, "worker_drain_timeout_sec", 0.1)
    import asyncio

    limiter = asyncio.Semaphore(0)  # a task still holds the only permit
    with pytest.raises(SystemExit):
        await shared._drain_and_exit(limiter, 1, logger)
    hook.assert_awaited_once()


async def test_a_pass_interrupted_by_shutdown_is_uncounted(redis_mock):
    from workers.merge_message import worker as merge

    await db.bump_merge_crashes("resp-1")
    merge._PASSES_IN_FLIGHT.add("resp-1")
    try:
        await merge._uncount_interrupted_passes()
    finally:
        merge._PASSES_IN_FLIGHT.discard("resp-1")
    assert await db.get_merge_crashes("resp-1") == 0


async def test_a_pass_that_really_crashed_stays_counted(redis_mock):
    """A process that dies never runs the hook; nothing else uncounts it."""
    await db.bump_merge_crashes("resp-2")
    assert await db.get_merge_crashes("resp-2") == 1
