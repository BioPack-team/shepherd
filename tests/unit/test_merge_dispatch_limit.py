"""merge_message's in-flight task limit must not shrink to its pool size.

The pool (sized to the pod's CPUs) is what bounds the CPU- and memory-heavy
merge work. A wake task spends most of its life off the pool: up to
``merge_debounce_sec`` waiting for more callbacks after taking the lock, Redis
round trips, or -- for a lock loser -- just acking. When the poll loop's task
limit was the pool size, a 2-CPU pod could have both slots asleep in the
debounce with its pool idle, capping it at about two queries a second while its
backlog grew past 300.
"""

import pytest

from workers.merge_message import worker as merge


@pytest.mark.parametrize("pool_workers", [1, 2, 4])
async def test_task_limit_exceeds_pool_size(monkeypatch, pool_workers):
    seen = {}

    class _Pool:
        def __init__(self, max_workers, *args, **kwargs):
            seen["pool"] = max_workers

        def shutdown(self):
            pass

    async def _run_poll_loop(stream, group, consumer, task_limit, handler, on_stop):
        seen["task_limit"] = task_limit

    monkeypatch.setattr(merge, "resolve_pool_workers", lambda limit, lg: pool_workers)
    monkeypatch.setattr(merge, "ProcessPoolManager", _Pool)
    monkeypatch.setattr(merge, "run_poll_loop", _run_poll_loop)

    await merge.poll_for_tasks()

    assert seen["pool"] == pool_workers
    assert seen["task_limit"] == merge.TASK_LIMIT
    assert seen["task_limit"] > seen["pool"]
