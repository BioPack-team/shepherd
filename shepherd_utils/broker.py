"""Shepherd Broker Manager."""

import asyncio
import logging

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from .config import settings

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
    socket_keepalive_options={},
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
    socket_keepalive_options={},
    health_check_interval=30,
    decode_responses=True,
    retry_on_timeout=True,
)

broker_client = aioredis.Redis(connection_pool=broker_redis_pool)
lock_client = aioredis.Redis(connection_pool=lock_redis_pool)


async def create_consumer_group(
    stream, group, logger: logging.Logger
):
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
        if messages:
            # logger.info(messages)
            stream, message_list = messages[0]
            return message_list[0]

    except Exception as e:
        logger.info(f"Failed to get task for {stream}, {e}")
        # wait a second before trying again, handle intermittent disconnections
        await asyncio.sleep(1)
        pass
    return None


async def mark_task_as_complete(
    stream, group, msg_id, logger: logging.Logger, retries=0
):
    """Send ACK message back to queue."""
    try:
        await broker_client.xack(stream, group, msg_id)

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
