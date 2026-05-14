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
"""

import asyncio
import json
import logging
import time

from .broker import broker_client

HEARTBEAT_PREFIX = "worker:heartbeat"
HEARTBEAT_SCAN_PATTERN = f"{HEARTBEAT_PREFIX}:*"
HEARTBEAT_INTERVAL_SEC = 5
HEARTBEAT_TTL_SEC = 15


def heartbeat_key(stream: str, consumer: str) -> str:
    return f"{HEARTBEAT_PREFIX}:{stream}:{consumer}"


class Heartbeat:
    """Background task that periodically refreshes a presence key in Redis."""

    def __init__(self, stream: str, consumer: str, task_limit: int):
        self.stream = stream
        self.consumer = consumer
        self.task_limit = task_limit
        self.started_at = time.time()
        self._task: asyncio.Task | None = None
        self._logger = logging.getLogger(f"shepherd.heartbeat.{stream}")

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
