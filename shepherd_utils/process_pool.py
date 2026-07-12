"""Process pool for CPU-bound worker overlays, with safe broken-pool handling.

A ``ProcessPoolExecutor`` whose child dies abruptly -- almost always a cgroup
OOM kill while a child processes a very large message -- becomes *permanently*
broken: every subsequent submission raises ``BrokenProcessPool``.

Rebuilding the pool in-process is unsafe. The executor is created and torn down
on the asyncio event loop thread, and doing that right after a child died can
wedge the loop: spawning replacement workers forks, which runs the at-fork lock
handlers on the loop thread, and tearing a broken executor down can block on its
internal shutdown lock. A wedged loop stops the worker's heartbeat, so the pod
looks alive to Kubernetes while it silently processes nothing and never recovers
-- exactly the failure this manager exists to avoid.

So instead of recovering in place, ``ProcessPoolManager`` fails the current task
and asks the worker to restart. On the first ``BrokenProcessPool`` it invokes the
``on_broken`` callback (wired to the graceful-drain shutdown): new work stops,
in-flight tasks finish -- their failures are ACKed via the normal
``handle_task_failure`` path, so nothing is left un-ACKed to be redelivered and
crash-loop -- and the process exits so Kubernetes brings the worker back with a
fresh pool. This mirrors the broker self-heal pattern (exit and let Kubernetes
reschedule) already used elsewhere.
"""

import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Callable, Optional


class ProcessPoolManager:
    """Owns a ``ProcessPoolExecutor`` and, on breakage, signals a clean restart.

    Args:
        max_workers: pool size handed to ``ProcessPoolExecutor``.
        max_tasks_per_child: optional ceiling on tasks a single child runs
            before it recycles, returning the memory a large message forced it
            to grow back to the OS. ``None`` (default) leaves children alive for
            the pool's lifetime; setting it makes the executor fall back to the
            ``spawn`` start method (a Python requirement of the parameter).
        name: label used in the log line emitted when the pool breaks.
        on_broken: zero-arg callback invoked once, the first time a child death
            breaks the pool. Wire it to ``shared.request_shutdown`` so the worker
            drains and exits for a Kubernetes restart. ``None`` just logs (the
            pool then stays broken, so only pass ``None`` where that's acceptable).
    """

    def __init__(
        self,
        max_workers: int,
        max_tasks_per_child: Optional[int] = None,
        name: str = "process pool",
        on_broken: Optional[Callable[[], None]] = None,
    ):
        self._max_workers = max_workers
        self._max_tasks_per_child = max_tasks_per_child
        self._name = name
        self._on_broken = on_broken
        self._lock = asyncio.Lock()
        self._broken = False
        self._executor = self._new_executor()

    def _new_executor(self) -> ProcessPoolExecutor:
        kwargs = {"max_workers": self._max_workers}
        if self._max_tasks_per_child is not None:
            kwargs["max_tasks_per_child"] = self._max_tasks_per_child
        return ProcessPoolExecutor(**kwargs)

    async def run(self, loop, fn, *args):
        """Run ``fn(*args)`` in the pool.

        On ``BrokenProcessPool`` this signals a restart (once) and re-raises, so
        the caller's task fails through the normal lifecycle (ACK + finish_query)
        while the worker drains and exits for a fresh pool.
        """
        try:
            return await loop.run_in_executor(self._executor, fn, *args)
        except BrokenProcessPool:
            await self._signal_broken()
            raise

    async def _signal_broken(self) -> None:
        async with self._lock:
            # Every future in flight on the dead pool raises at once, so several
            # tasks may land here; only the first trips the signal.
            if self._broken:
                return
            self._broken = True
        logging.error(
            f"{self._name} broke (a child died or was OOM-killed). Rebuilding it "
            "in-process risks deadlocking the event loop, so requesting a clean "
            "worker restart instead; the failing task goes to finish_query."
        )
        if self._on_broken is not None:
            try:
                self._on_broken()
            except Exception:
                logging.exception(f"{self._name}: on_broken handler failed")

    @property
    def is_broken(self) -> bool:
        return self._broken

    def shutdown(self) -> None:
        """Tear the current executor down (best effort)."""
        try:
            self._executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
