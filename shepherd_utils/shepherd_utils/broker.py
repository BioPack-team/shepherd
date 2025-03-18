"""Redis Broker Manager."""
import asyncio
import redis.asyncio as aioredis
from shepherd_utils.config import settings

broker_redis_pool = aioredis.BlockingConnectionPool(
    host=settings.redis_host,
    port=settings.redis_port,
    db=0,
    password=settings.redis_password,
    max_connections=10,
    timeout=600,
    decode_responses=True,
)


async def create_consumer_group(redis_client, stream, group):
    """Ensure a redis consumer group exists."""
    try:
        await redis_client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception:
        pass


async def ara_queue(ara_target, query_id):
    """Put a query_id on the queue for an ARA."""
    try:
        client = await aioredis.Redis(
            connection_pool=broker_redis_pool,
        )
        # print(f"Putting {query_id} on {ara_target} stream")
        await client.xadd(ara_target, {"query_id": query_id})
        await client.close()
    except Exception as e:
        # failed to put message on ara stream
        # TODO: do something more severe
        print(f"Failed to put it on there {e}")
        pass


async def get_ara_task(ara_target, logger):
    """Get an ara task from the queue."""
    try:
        client = await aioredis.Redis(
            connection_pool=broker_redis_pool,
        )
        await create_consumer_group(client, ara_target, ara_target)
        # logger.info(f"Getting task for {ara_target}")
        messages = await client.xreadgroup(
            ara_target, ara_target, {ara_target: ">"}, count=1, block=5000
        )
        if messages:
            # logger.info(messages)
            stream, message_list = messages[0]
            return message_list[0]
            # for msg_id, msg_data in message_list:
            #     logger.info(msg_data)
            #     await asyncio.sleep(2)
            #     await client.xack(ara_target, ara_target, msg_id)

    except Exception as e:
        logger.info(f"Failed to get task for {ara_target}, {e}")
        pass


async def mark_task_as_complete(ara_target, msg_id, logger, retries=0):
    """Send ACK message back to queue."""
    try:
        client = await aioredis.Redis(
            connection_pool=broker_redis_pool,
        )
        await client.xack(ara_target, ara_target, msg_id)

    except Exception as e:
        logger.info(f"Failed to mark task for completion. Trying again {ara_target}, {e}")
        retries += 1
        if retries < 5:
            await mark_task_as_complete(ara_target, msg_id, logger, retries)
        else:
            logger.error(f"[{msg_id}] Failed to successfully ACK message even though it was completed.")
