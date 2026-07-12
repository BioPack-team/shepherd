"""Self-healing process pool for CPU-bound worker overlays.

A ``ProcessPoolExecutor`` whose child dies abruptly -- almost always a cgroup
OOM kill while a child processes a very large message -- becomes *permanently*
broken: every subsequent submission raises ``BrokenProcessPool``. A worker that
keeps one executor for its whole lifetime would then fail every task after the
first such death until the pod restarted.

``ProcessPoolManager`` catches the breakage, tears the dead pool down, and stands
up a fresh one so the worker recovers on its next task. The task whose child died
still fails cleanly (its ``run`` call re-raises and ``run_task_lifecycle`` routes
it to ``finish_query`` with an error); only the poisoning of *later* tasks is
fixed.

The pool uses the **spawn** start method (matching the merge_message worker).
That matters for correctness, not just style: with the default *fork* method the
executor spawns replacement children by calling ``os.fork()`` on the asyncio
event loop thread, which runs the registered at-fork handlers there. Right after
a child died abruptly, acquiring those locks on the loop thread can deadlock --
the loop wedges, the heartbeat stops, and the pod looks alive while doing nothing
and never recovering. Spawn launches children via ``fork_exec`` (C level), which
does not run the Python at-fork handlers, so rebuilding the pool in-process is
safe.
"""

import asyncio
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Optional


def _init_pool_worker() -> None:
    """Initializer run once per spawned worker process."""
    import faulthandler

    # Dump C-level tracebacks to stderr on a segfault / abort so a child crash
    # leaves something actionable in the logs instead of a bare
    # BrokenProcessPool in the parent.
    faulthandler.enable()
    # Spawned children start with no logging config; give them a minimal one so
    # log calls inside the child reach stderr for the container log collector.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [worker %(process)d] %(levelname)s %(name)s: %(message)s",
    )


class ProcessPoolManager:
    """Owns a spawn-context ``ProcessPoolExecutor`` and replaces it once broken.

    Args:
        max_workers: pool size handed to ``ProcessPoolExecutor``.
        max_tasks_per_child: optional ceiling on tasks a single child runs
            before it recycles, returning the memory a large message forced it
            to grow back to the OS. ``None`` (default) leaves children alive for
            the pool's lifetime.
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
        # Spawn: see module docstring -- fork would risk deadlocking the event
        # loop thread when the pool is rebuilt after a child death.
        self._ctx = multiprocessing.get_context("spawn")
        self._executor = self._new_executor()

    def _new_executor(self) -> ProcessPoolExecutor:
        kwargs = {
            "max_workers": self._max_workers,
            "mp_context": self._ctx,
            "initializer": _init_pool_worker,
        }
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
                f"{self._name} broke (a child died or was OOM-killed on a large "
                "message); replacing it so the worker keeps processing tasks."
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
