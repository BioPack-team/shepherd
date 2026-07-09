"""Worker heartbeat helper.

Every worker that calls ``get_tasks`` automatically registers itself in Redis
with a short-lived key. The monitor service scans these keys to know how many
workers of each type are alive, when they started, and what their capacity is.
Workers self-report so the monitor doesn't have to introspect Docker or
Kubernetes -- this works identically under either, and handles autoscaling.

Key format: ``worker:heartbeat:{stream}:{consumer}``
TTL: refreshed every ``HEARTBEAT_INTERVAL_SEC``, expires after
``HEARTBEAT_TTL_SEC``. If a worker crashes or is killed, the key disappears
within the TTL window and the monitor will surface it as a worker-loss event.

On SIGTERM/SIGINT we additionally write a *shutdown marker*
``worker:shutdown:{stream}:{consumer}`` synchronously before exiting. The
monitor uses this marker to tell a clean scale-down (marker present) apart
from a crash (marker absent). Markers are written with a plain sync redis
client because Python signal handlers can't safely drive the asyncio loop.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import threading
import time

from .broker import broker_client
from .config import settings

HEARTBEAT_PREFIX = "worker:heartbeat"
HEARTBEAT_SCAN_PATTERN = f"{HEARTBEAT_PREFIX}:*"
HEARTBEAT_INTERVAL_SEC = 5
HEARTBEAT_TTL_SEC = 15

SHUTDOWN_PREFIX = "worker:shutdown"
SHUTDOWN_SCAN_PATTERN = f"{SHUTDOWN_PREFIX}:*"
# Marker outlives the heartbeat TTL with a wide margin so the monitor reliably
# observes "marker present" at the moment the heartbeat disappears.
SHUTDOWN_TTL_SEC = 120


def heartbeat_key(stream: str, consumer: str) -> str:
    return f"{HEARTBEAT_PREFIX}:{stream}:{consumer}"


def shutdown_key(stream: str, consumer: str) -> str:
    return f"{SHUTDOWN_PREFIX}:{stream}:{consumer}"


def _read_rss_bytes() -> "int | None":
    """Resident set size of this process in bytes, read from ``/proc/self``.

    Dependency-free (no psutil) so it adds nothing to worker images. Returns
    ``None`` off Linux or if ``/proc`` isn't readable.
    """
    try:
        with open("/proc/self/statm", encoding="ascii") as f:
            # Fields are in pages; the second is resident set size.
            rss_pages = int(f.read().split()[1])
        return rss_pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return None


def _read_cpu_seconds() -> "float | None":
    """Cumulative CPU time (user + system) of this process in seconds.

    Parsed from ``/proc/self/stat``. The ``comm`` field can contain spaces and
    parentheses, so we split after the final ``)`` to keep field offsets stable.
    """
    try:
        with open("/proc/self/stat", encoding="ascii") as f:
            data = f.read()
        # Everything after the last ')' starts at the ``state`` field (field 3),
        # so utime (field 14) is index 11 and stime (field 15) is index 12.
        rest = data[data.rfind(")") + 2 :].split()
        ticks = int(rest[11]) + int(rest[12])
        return ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError, ZeroDivisionError):
        return None


class Heartbeat:
    """Background task that periodically refreshes a presence key in Redis."""

    def __init__(
        self,
        stream: str,
        consumer: str,
        task_limit: int,
        manage_signals: bool = True,
        limiter: "asyncio.Semaphore | None" = None,
    ):
        self.stream = stream
        self.consumer = consumer
        self.task_limit = task_limit
        self.started_at = time.time()
        # The concurrency semaphore ``get_tasks`` owns. We read its available
        # permits to report how many tasks are currently in flight; left None
        # (e.g. in tests) the in-flight count is simply omitted.
        self._limiter = limiter
        # Previous CPU sample, so each ping can report utilization over the
        # interval since the last one rather than since process start.
        self._last_cpu_sec: float | None = None
        self._last_cpu_wall: float | None = None
        self._task: asyncio.Task | None = None
        self._logger = logging.getLogger(f"shepherd.heartbeat.{stream}")
        # When False, this Heartbeat does not install its own SIGTERM/SIGINT
        # handlers -- the caller (shared.get_tasks) installs asyncio-aware
        # handlers instead so it can drain in-flight tasks before exiting.
        self.manage_signals = manage_signals
        self._signal_installed = False
        self._prev_handlers: dict = {}

    def _in_flight(self) -> "int | None":
        """Tasks currently running = task_limit minus the semaphore's free permits."""
        if self._limiter is None:
            return None
        available = getattr(self._limiter, "_value", None)
        if available is None:
            return None
        return max(0, self.task_limit - int(available))

    def _cpu_pct(self) -> "float | None":
        """Percent of a single core used since the previous ping (top-style; can
        exceed 100 on multi-core work)."""
        now_wall = time.time()
        now_cpu = _read_cpu_seconds()
        pct: float | None = None
        if (
            now_cpu is not None
            and self._last_cpu_sec is not None
            and self._last_cpu_wall is not None
        ):
            elapsed = now_wall - self._last_cpu_wall
            if elapsed > 0:
                pct = max(0.0, round(100.0 * (now_cpu - self._last_cpu_sec) / elapsed, 1))
        self._last_cpu_sec = now_cpu
        self._last_cpu_wall = now_wall
        return pct

    async def _ping(self) -> None:
        payload = json.dumps(
            {
                "stream": self.stream,
                "consumer": self.consumer,
                "started_at": self.started_at,
                "last_seen": time.time(),
                "task_limit": self.task_limit,
                "in_flight": self._in_flight(),
                "rss_bytes": _read_rss_bytes(),
                "cpu_pct": self._cpu_pct(),
            }
        )
        try:
            await broker_client.set(
                heartbeat_key(self.stream, self.consumer),
                payload,
                ex=HEARTBEAT_TTL_SEC,
            )
        except Exception as e:
            self._logger.debug(f"Heartbeat ping failed: {e}")

    async def _loop(self) -> None:
        while True:
            await self._ping()
            await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)

    def start(self) -> "Heartbeat":
        if self._task is None:
            self._task = asyncio.create_task(self._loop())
        if self.manage_signals:
            self._install_signal_handlers()
        return self

    async def mark_clean_shutdown(self) -> None:
        """Write the shutdown marker from within the event loop.

        Mirror of ``_mark_shutdown_sync`` for the graceful-shutdown path, which
        already runs on the asyncio loop and so can use the async broker client.
        The monitor reads this marker to classify the disappearance of the
        heartbeat as a clean scale-down rather than a crash. Heartbeat key
        deletion is handled by ``stop()``.
        """
        payload = json.dumps(
            {
                "stream": self.stream,
                "consumer": self.consumer,
                "signum": int(signal.SIGTERM),
                "ts": time.time(),
            }
        )
        try:
            await broker_client.set(
                shutdown_key(self.stream, self.consumer),
                payload,
                ex=SHUTDOWN_TTL_SEC,
            )
        except Exception as e:
            self._logger.debug(f"Failed to write clean shutdown marker: {e}")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        try:
            await broker_client.delete(heartbeat_key(self.stream, self.consumer))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Clean-shutdown signal handling
    #
    # ``signal.signal`` is the only safe way to act on SIGTERM here -- the
    # asyncio loop is already running by the time get_tasks calls us, and a
    # signal handler can't await on the loop without races. We instead do a
    # blocking SET via a sync redis client so the marker is durable before the
    # process exits, then chain to whatever handler was installed before us.
    # ------------------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        if self._signal_installed:
            return
        if threading.current_thread() is not threading.main_thread():
            return
        for sig_num in (signal.SIGTERM, signal.SIGINT):
            try:
                self._prev_handlers[sig_num] = signal.getsignal(sig_num)
                signal.signal(sig_num, self._signal_handler)
            except (ValueError, OSError):
                pass
        self._signal_installed = True

    def _signal_handler(self, signum, frame) -> None:
        self._mark_shutdown_sync(signum)
        prev = self._prev_handlers.get(signum, signal.SIG_DFL)
        if callable(prev):
            try:
                prev(signum, frame)
            except (SystemExit, KeyboardInterrupt):
                raise
            except Exception:
                pass
        elif prev == signal.SIG_DFL:
            # Re-raise as the default signal so the process actually exits.
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)
        # SIG_IGN: do nothing extra.

    def _mark_shutdown_sync(self, signum) -> None:
        try:
            # Lazy import so the sync client doesn't get created at module
            # import time inside workers that never receive a signal.
            import redis as sync_redis

            client = sync_redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=0,
                password=settings.redis_password,
                socket_timeout=2,
                socket_connect_timeout=2,
            )
            payload = json.dumps(
                {
                    "stream": self.stream,
                    "consumer": self.consumer,
                    "signum": int(signum),
                    "ts": time.time(),
                }
            )
            client.set(
                shutdown_key(self.stream, self.consumer),
                payload,
                ex=SHUTDOWN_TTL_SEC,
            )
            client.delete(heartbeat_key(self.stream, self.consumer))
            client.close()
        except Exception:
            # Best-effort: if we can't reach Redis at shutdown there's nothing
            # useful to do other than exit. Monitor will then classify this as
            # a crash, which is the safe default.
            pass
