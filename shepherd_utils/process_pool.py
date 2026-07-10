"""Self-healing process pool for CPU-bound worker overlays.

A ``ProcessPoolExecutor`` that loses a child abruptly -- almost always a kernel
OOM kill while a child processes a very large message -- becomes *permanently*
broken: every subsequent submission raises ``BrokenProcessPool``, not only the
task whose child died. A worker that creates one executor for its whole
lifetime (as ``poll_for_tasks`` does) would therefore fail every task after the
first such death -- with "A process in the process pool was terminated abruptly
while the future was running or pending" -- until the pod was restarted.

``ProcessPoolManager`` wraps the executor, catches the breakage, tears the dead
pool down, and stands up a fresh one so the worker recovers on its next task.
The task whose child died still fails cleanly (its ``run`` call re-raises, and
``run_task_lifecycle`` routes it to ``finish_query`` with an error); only the
poisoning of *later* tasks is fixed.
"""

import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Optional


class ProcessPoolManager:
    """Owns a ``ProcessPoolExecutor`` and transparently replaces it once broken.

    Args:
        max_workers: pool size handed to ``ProcessPoolExecutor``.
        max_tasks_per_child: optional ceiling on tasks a single child runs
            before it recycles, returning the memory a large message forced it
            to grow back to the OS. ``None`` (default) leaves children alive for
            the pool's lifetime; setting it makes the executor fall back to the
            ``spawn`` start method (a Python requirement of the parameter).
        name: label used in the log line emitted when the pool is replaced.
    """

    def __init__(
        self,
        max_workers: int,
        max_tasks_per_child: Optional[int] = None,
        name: str = "process pool",
    ):
        self._max_workers = max_workers
        self._max_tasks_per_child = max_tasks_per_child
        self._name = name
        self._lock = asyncio.Lock()
        self._executor = self._new_executor()

    def _new_executor(self) -> ProcessPoolExecutor:
        kwargs = {"max_workers": self._max_workers}
        if self._max_tasks_per_child is not None:
            kwargs["max_tasks_per_child"] = self._max_tasks_per_child
        return ProcessPoolExecutor(**kwargs)

    async def run(self, loop, fn, *args):
        """Run ``fn(*args)`` in the pool, recreating it if the call broke it."""
        executor = self._executor
        try:
            return await loop.run_in_executor(executor, fn, *args)
        except BrokenProcessPool:
            # A child died; the whole executor is now unusable. Replace it before
            # re-raising so this task fails cleanly (handled upstream) while the
            # next task gets a healthy pool.
            await self._replace(executor)
            raise

    async def _replace(self, broken: ProcessPoolExecutor) -> None:
        async with self._lock:
            # When a child dies, every future in flight on that pool raises
            # BrokenProcessPool at once, so several tasks may race here. Only the
            # first to see this particular pool swaps it; the rest are no-ops.
            if self._executor is not broken:
                return
            logging.error(
                f"{self._name} broke (a child likely died or was OOM-killed on a "
                "large message); replacing it so the worker keeps processing tasks."
            )
            try:
                broken.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            self._executor = self._new_executor()

    def shutdown(self) -> None:
        """Tear the current executor down (best effort)."""
        try:
            self._executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
