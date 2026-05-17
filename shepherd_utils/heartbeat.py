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


class Heartbeat:
    """Background task that periodically refreshes a presence key in Redis."""

    def __init__(self, stream: str, consumer: str, task_limit: int):
        self.stream = stream
        self.consumer = consumer
        self.task_limit = task_limit
        self.started_at = time.time()
        self._task: asyncio.Task | None = None
        self._logger = logging.getLogger(f"shepherd.heartbeat.{stream}")
        self._signal_installed = False
        self._prev_handlers: dict = {}

    async def _ping(self) -> None:
        payload = json.dumps(
            {
                "stream": self.stream,
                "consumer": self.consumer,
                "started_at": self.started_at,
                "last_seen": time.time(),
                "task_limit": self.task_limit,
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
        self._install_signal_handlers()
        return self

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

