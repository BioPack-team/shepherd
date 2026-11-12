"""merge_message's in-flight task limit must not shrink to its pool size.

The pool (sized to the pod's CPUs) is what bounds the CPU- and memory-heavy
merge work. Part of a wake task's life is off the pool: Redis round trips, or
-- for a lock loser -- just acking. When the poll loop's task limit was the
pool size, those tasks held every slot while the pool sat idle, and on a 2-CPU
pod (then also sleeping in a 1s pre-merge debounce) the backlog grew past 300.
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
