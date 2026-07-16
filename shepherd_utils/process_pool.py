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

It also bounds each task with ``task_timeout``. A child that *hangs* (an
infinite loop, a pathological O(n^2) blow-up) rather than dying would otherwise
hold its pool slot forever -- and with a small pool, silently stall the whole
worker while the heartbeat keeps beating. On timeout the stuck child is killed,
the pool is rebuilt, and the task fails like any other error.

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
import os
import signal
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Optional

# Grace between SIGABRT (which makes the child's faulthandler dump its stack)
# and the SIGKILL fallback, so the traceback has time to reach stderr.
_ABORT_GRACE_SEC = 0.5


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


def _consume_future_exception(fut) -> None:
    """Retrieve a discarded future's result/exception so asyncio doesn't warn.

    After a timeout we stop awaiting the future but leave the child to be killed;
    the future then resolves (usually with ``BrokenProcessPool``) with nobody to
    read it. Reading it here suppresses the "exception was never retrieved" log.
    """
    try:
        if not fut.cancelled():
            fut.exception()
    except Exception:
        pass


class ProcessPoolManager:
    """Owns a spawn-context ``ProcessPoolExecutor``; self-heals and time-bounds.

    Args:
        max_workers: pool size handed to ``ProcessPoolExecutor``.
        max_tasks_per_child: ceiling on tasks a single child runs before it
            recycles, returning the memory a large message forced it to grow
            back to the OS. ``None`` or <= 0 leaves children alive for the
            pool's lifetime.
        name: label used in the log line emitted when the pool is replaced.
        task_timeout: per-task ceiling in seconds. When a task exceeds it, the
            running child is killed, the pool is rebuilt, and ``run`` raises
            ``asyncio.TimeoutError``. ``None`` / <= 0 disables the timeout.
    """

    def __init__(
        self,
        max_workers: int,
        max_tasks_per_child: Optional[int] = None,
        name: str = "process pool",
        task_timeout: Optional[float] = None,
    ):
        self._max_workers = max_workers
        self._max_tasks_per_child = max_tasks_per_child
        self._name = name
        self._task_timeout = task_timeout
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
        # ProcessPoolExecutor rejects max_tasks_per_child <= 0, so only pass a
        # positive value; None / 0 / negative means "don't recycle".
        if self._max_tasks_per_child and self._max_tasks_per_child > 0:
            kwargs["max_tasks_per_child"] = self._max_tasks_per_child
        return ProcessPoolExecutor(**kwargs)

    async def run(self, loop, fn, *args):
        """Run ``fn(*args)`` in the pool, self-healing on breakage or timeout."""
        executor = self._executor
        timeout = self._task_timeout

        if not timeout or timeout <= 0:
            # No timeout: await the offloaded call directly.
            try:
                return await loop.run_in_executor(executor, fn, *args)
            except BrokenProcessPool:
                await self._replace(executor, reason="a child died or was OOM-killed")
                raise

        # Timeout path. NOTE: asyncio.wait_for is deliberately NOT used here --
        # on timeout it cancels then *awaits* the future, and a running process
        # task can't be cancelled, so it would block until the runaway child
        # finishes (hours), defeating the timeout. asyncio.wait returns after the
        # window without awaiting the still-pending future, so we can kill the
        # child and move on.
        future = loop.run_in_executor(executor, fn, *args)
        try:
            done, pending = await asyncio.wait({future}, timeout=timeout)
        except BaseException:
            # e.g. this task was cancelled (shutdown); don't leave the future's
            # eventual result unretrieved.
            future.add_done_callback(_consume_future_exception)
            raise

        if pending:
            future.add_done_callback(_consume_future_exception)
            logging.error(
                f"{self._name}: task exceeded {timeout:.0f}s; killing the stuck "
                "child and rebuilding the pool. The task is failed."
            )
            await self._replace(executor, reason=f"a task exceeded {timeout:.0f}s")
            raise asyncio.TimeoutError(
                f"{self._name} task exceeded the {timeout:.0f}s limit"
            )

        try:
            return future.result()
        except BrokenProcessPool:
            await self._replace(executor, reason="a child died or was OOM-killed")
            raise

    async def _kill_children(self, executor: ProcessPoolExecutor) -> None:
        """Force-terminate any live children of ``executor``, capturing stacks.

        A ProcessPoolExecutor gives no way to cancel a running child, so on a
        timeout we terminate the pool's children directly to stop the runaway
        work and free the slot. We send SIGABRT first, not SIGKILL: the
        faulthandler installed in ``_init_pool_worker`` catches SIGABRT and dumps
        the child's C+Python traceback to stderr before it dies -- so a child
        that hung *before* it ever ran its task (e.g. stuck in cold-start
        import/setup) leaves an actionable stack instead of a silent kill. After
        a short grace for that dump, any child still alive (ignored SIGABRT, e.g.
        stuck in an uninterruptible syscall) is SIGKILLed so the slot is always
        freed. Already-dead children (the broken-pool case) are a no-op.
        ``_processes`` is a CPython internal (pid -> Process); guarded.
        """
        procs = list((getattr(executor, "_processes", None) or {}).values())
        aborted = []
        for proc in procs:
            pid = getattr(proc, "pid", None)
            if pid is None:
                continue
            try:
                os.kill(pid, signal.SIGABRT)
                aborted.append(proc)
            except (ProcessLookupError, OSError):
                pass
        if not aborted:
            return
        # Let faulthandler write the traceback(s) and the children abort.
        await asyncio.sleep(_ABORT_GRACE_SEC)
        for proc in aborted:
            try:
                if proc.is_alive():
                    proc.kill()
            except Exception:
                pass

    async def _replace(
        self, broken: ProcessPoolExecutor, reason: str = "pool replaced"
    ) -> None:
        async with self._lock:
            # When a child dies, every future in flight on that pool raises
            # BrokenProcessPool at once, so several tasks may race here. Only the
            # first to see this particular pool swaps it; the rest are no-ops.
            if self._executor is not broken:
                return
            logging.error(
                f"{self._name}: replacing process pool ({reason}); killing any "
                "live children and standing up a fresh pool."
            )
            await self._kill_children(broken)
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
