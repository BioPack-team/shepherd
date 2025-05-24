"""Shepherd Broker Manager."""

import asyncio
import logging
from redis.exceptions import ResponseError
import redis.asyncio as aioredis
from .config import settings

broker_redis_pool = aioredis.BlockingConnectionPool(
    host=settings.redis_host,
    port=settings.redis_port,
    db=0,
    password=settings.redis_password,
    max_connections=10,
    timeout=600,
    decode_responses=True,
)

lock_redis_pool = aioredis.BlockingConnectionPool(
    host=settings.redis_host,
    port=settings.redis_port,
    db=2,
    password=settings.redis_password,
    max_connections=10,
    timeout=600,
    decode_responses=True,
)


async def create_consumer_group(
    redis_client: aioredis.Redis, stream, group, logger: logging.Logger
):
    """Ensure a redis consumer group exists."""
    try:
        await redis_client.xgroup_create(stream, group, "0", mkstream=True)
    except ResponseError:
        # this gets called every time we poll for new tasks and will throw an error if the group already exists
        pass
    except Exception as e:
        logger.warning(f"Failed to create consumer group: {e}")
        pass


async def add_task(queue, payload, logger: logging.Logger):
    """Put a payload on the queue for a worker to pick up."""
    try:
        client = await aioredis.Redis(
            connection_pool=broker_redis_pool,
        )
        # print(f"Putting {payload} on {queue} stream")
        await client.xadd(queue, payload)
        await client.aclose()
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
        client = await aioredis.Redis(
            connection_pool=broker_redis_pool,
        )
        await create_consumer_group(client, stream, group, logger)
        # logger.info(f"Getting task for {ara_target}")
        messages = await client.xreadgroup(
            group, consumer, {stream: ">"}, count=1, block=5000
        )
        if messages:
            # logger.info(messages)
            stream, message_list = messages[0]
            return message_list[0]
        await client.aclose()

    except Exception as e:
        logger.info(f"Failed to get task for {stream}, {e}")
        pass
    return None


async def mark_task_as_complete(
    stream, group, msg_id, logger: logging.Logger, retries=0
):
    """Send ACK message back to queue."""
    try:
        client = await aioredis.Redis(
            connection_pool=broker_redis_pool,
        )
        await client.xack(stream, group, msg_id)
        await client.aclose()

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
    try:
        client = await aioredis.Redis(
            connection_pool=lock_redis_pool,
        )
        locked = await client.get(response_id)
        if locked is None:
            await client.setex(response_id, 45, consumer_id)
            await client.aclose()
            return True
        for i in range(60):
            await asyncio.sleep(5)
            locked = await client.get(response_id)
            if locked is None:
                await client.setex(response_id, 45, consumer_id)
                await client.aclose()
                return True
        return False
    except Exception as e:
        logger.error(f"Failed to successfully lock message: {e}")
        return False


async def remove_lock(
    response_id: str,
    consumer_id: str,
    logger: logging.Logger,
):
    """Acquire a redis lock for a given row."""
    try:
        client = await aioredis.Redis(
            connection_pool=lock_redis_pool,
        )
        locked = await client.get(response_id)
        if locked is None:
            logger.error("Something happened and we don't have the lock anymore.")
        elif locked != consumer_id:
            logger.error("A different consumer has a lock on this entry.")
        else:
            await client.delete(response_id)
        await client.aclose()
    except Exception as e:
        logger.error(f"Failed to successfully unlock message: {e}")
