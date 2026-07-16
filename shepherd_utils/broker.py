"""Shepherd Broker Manager."""

import asyncio
import logging
import socket
import time

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from .config import settings


def _keepalive_options() -> dict:
    """TCP keepalive tuning so a dead broker connection is detected in ~tens of
    seconds instead of relying on Linux's 2-hour default keepalive idle time.

    Without these, a half-open connection -- the broker pod moved, or a stateful
    firewall/conntrack entry silently dropped the flow -- is only noticed via the
    per-command ``socket_timeout``, and reconnecting keeps hitting the same dead
    endpoint. Probing the peer actively lets the kernel tear the socket down so
    redis-py rebuilds it against a live endpoint. Only options the running
    platform defines are included (``TCP_KEEPIDLE`` and friends are Linux-only;
    macOS spells it ``TCP_KEEPALIVE``), so importing this on a dev laptop or in
    CI doesn't raise. Net effect on Linux: ~30s idle + 3 failed probes 10s apart
    => a dead connection is reaped in ~60s.
    """
    opts = {}
    if hasattr(socket, "TCP_KEEPIDLE"):
        opts[socket.TCP_KEEPIDLE] = 30
    if hasattr(socket, "TCP_KEEPINTVL"):
        opts[socket.TCP_KEEPINTVL] = 10
    if hasattr(socket, "TCP_KEEPCNT"):
        opts[socket.TCP_KEEPCNT] = 3
    return opts


_KEEPALIVE_OPTIONS = _keepalive_options()

broker_redis_pool = aioredis.BlockingConnectionPool(
    host=settings.redis_host,
    port=settings.redis_port,
    db=0,
    password=settings.redis_password,
    max_connections=10,
    timeout=30,
    socket_timeout=7,  # Needs to be greater than get_task xgroupread timeout
    socket_connect_timeout=10,
    socket_keepalive=True,
    socket_keepalive_options=_KEEPALIVE_OPTIONS,
    health_check_interval=30,
    decode_responses=True,
    retry_on_timeout=True,
)

lock_redis_pool = aioredis.BlockingConnectionPool(
    host=settings.redis_host,
    port=settings.redis_port,
    db=2,
    password=settings.redis_password,
    max_connections=10,
    timeout=30,
    socket_timeout=5,
    socket_connect_timeout=10,
    socket_keepalive=True,
    socket_keepalive_options=_KEEPALIVE_OPTIONS,
    health_check_interval=30,
    decode_responses=True,
    retry_on_timeout=True,
)

broker_client = aioredis.Redis(connection_pool=broker_redis_pool)
lock_client = aioredis.Redis(connection_pool=lock_redis_pool)


class BrokerHealth:
    """Tracks how long this worker has gone without a successful broker read.

    ``get_task`` records a success on every completed read (an empty read counts
    -- the broker answered) and a failure on every exception. The worker poll
    loop consults ``seconds_since_success`` to decide when the broker has been
    unreachable long enough that *this* worker is wedged and should exit for
    Kubernetes to replace it with a fresh connection.

    Initialized as "just succeeded" so a worker that starts up while the broker
    is unreachable still gets a full grace window before exiting -- otherwise a
    fleet-wide outage would send every freshly-restarted pod straight back into
    a crash loop.
    """

    def __init__(self) -> None:
        self._last_success = time.monotonic()
        self.consecutive_failures = 0

    def record_success(self) -> None:
        self._last_success = time.monotonic()
        self.consecutive_failures = 0

    def record_failure(self) -> None:
        self.consecutive_failures += 1

    def seconds_since_success(self) -> float:
        return time.monotonic() - self._last_success


# Module-level singleton: one worker process has exactly one broker pool, so one
# health view. Reset in tests via ``broker_health.record_success()``.
broker_health = BrokerHealth()


async def create_consumer_group(stream, group, logger: logging.Logger):
    """Ensure a redis consumer group exists."""
    try:
        await broker_client.xgroup_create(stream, group, "0", mkstream=True)
    except ResponseError:
        # this gets called every time we poll for new tasks and will throw an error if the group already exists
        pass
    except Exception as e:
        logger.warning(f"Failed to create consumer group: {e}")
        pass


async def add_task(queue, payload, logger: logging.Logger):
    """Put a payload on the queue for a worker to pick up."""
    try:
        # print(f"Putting {payload} on {queue} stream")
        await broker_client.xadd(queue, payload)
    except Exception as e:
        # failed to put message on ara stream
        # TODO: do something more severe
        logger.error(
            f"Failed to put new task on the queue: {e}, inputs: {queue}, {payload}"
        )
        pass


async def get_task(stream, group, consumer, logger: logging.Logger):
    """Get an ara task from the queue."""
    try:
        await create_consumer_group(stream, group, logger)
        # logger.info(f"Getting task for {ara_target}")
        messages = await broker_client.xreadgroup(
            group, consumer, {stream: ">"}, count=1, block=5000
        )
        # The broker answered -- an empty read is still a healthy read.
        broker_health.record_success()
        if messages:
            # logger.info(messages)
            stream, message_list = messages[0]
            return message_list[0]

    except Exception as e:
        broker_health.record_failure()
        logger.info(f"Failed to get task for {stream}, {e}")
        # wait a second before trying again, handle intermittent disconnections
        await asyncio.sleep(1)
        pass
    return None


async def mark_task_as_complete(
    stream, group, msg_id, logger: logging.Logger, retries=0
):
    """Send ACK message back to queue, then delete the entry.

    Streams aren't used for replay/audit here, so once a message has been
    successfully processed we delete it to keep ``XLEN`` bounded. The XDEL is
    best-effort: a failure leaves the message acked-but-present, which a
    periodic janitor in the monitor cleans up.
    """
    try:
        await broker_client.xack(stream, group, msg_id)
        try:
            await broker_client.xdel(stream, msg_id)
        except Exception as e:
            logger.debug(f"XDEL failed for {msg_id} in {stream}: {e}")

    except Exception as e:
        retries += 1
        logger.info(
            f"Failed to mark task {msg_id} in stream {stream} as complete. Try #{retries}. Trying again, {e}"
        )
        if retries < 5:
            await mark_task_as_complete(stream, group, msg_id, logger, retries)
        else:
            logger.error(
                f"[{msg_id}] Failed to successfully ACK message even though it was completed."
            )


async def try_lock(
    response_id: str,
    consumer_id: str,
    logger: logging.Logger,
    ttl_sec: int = 45,
) -> bool:
    """Non-blocking lock attempt: a single ``SET NX EX``.

    Unlike ``acquire_lock`` this never waits for a held lock to be released. The
    merge_message worker uses it so that a worker which loses the race for a
    query's lock returns immediately to do other work instead of sitting idle --
    the worker that holds the lock drains the whole query anyway.
    """
    try:
        acquired = await lock_client.set(response_id, consumer_id, ex=ttl_sec, nx=True)
        return bool(acquired)
    except Exception as e:
        logger.error(f"Failed to attempt lock: {e}")
        return False


async def acquire_lock(
    response_id: str,
    consumer_id: str,
    logger: logging.Logger,
):
    """Acquire a redis lock for a given row."""
    pubsub = None
    got_lock = False
    try:
        pubsub = lock_client.pubsub()
        await pubsub.subscribe(response_id)
        for i in range(12):
            acquired = await lock_client.set(response_id, consumer_id, ex=45, nx=True)
            if acquired:
                got_lock = True
                break
            try:
                await pubsub.get_message(ignore_subscribe_messages=True, timeout=5)
            except asyncio.TimeoutError:
                logger.debug(f"Timed out trying to get lock on try {i}")
                pass
            # await asyncio.sleep(1)
            # try again

    except Exception as e:
        logger.error(f"Failed to successfully lock message: {e}")
    finally:
        if pubsub is not None:
            await pubsub.unsubscribe(response_id)
            await pubsub.aclose()
        return got_lock


UNLOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    redis.call("del", KEYS[1])
    redis.call("publish", KEYS[1], "released")
    return 1
else
    return 0
end
"""


async def remove_lock(
    response_id: str,
    consumer_id: str,
    logger: logging.Logger,
):
    """Acquire a redis lock for a given row."""
    try:
        unlock_script = lock_client.register_script(UNLOCK_SCRIPT)
        await unlock_script(keys=[response_id], args=[consumer_id])
    except Exception as e:
        logger.error(f"Failed to successfully unlock message: {e}")


REFRESH_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"""


async def refresh_lock(
    response_id: str,
    consumer_id: str,
    ttl_ms: int,
    logger: logging.Logger,
) -> bool:
    """Extend our own lock's TTL (compare-and-PEXPIRE).

    Only refreshes if we still hold the lock, so a lock that already expired and
    was re-acquired by someone else is never clobbered. Used by the merge worker
    to keep a long drain from letting the lock lapse mid-merge.
    """
    try:
        refresh_script = lock_client.register_script(REFRESH_SCRIPT)
        result = await refresh_script(keys=[response_id], args=[consumer_id, ttl_ms])
        return bool(result)
    except Exception as e:
        logger.error(f"Failed to refresh lock: {e}")
        return False
