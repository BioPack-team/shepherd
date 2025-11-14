"""Mark a query as completed and do any callbacks."""

import asyncio
import httpx
import logging
import time
import uuid


from shepherd_utils.broker import mark_task_as_complete
from shepherd_utils.db import (
    get_logs,
    get_message,
    get_query_state,
    set_query_completed,
)
from shepherd_utils.shared import get_tasks
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "finish_query"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def finish_query(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    response_id = task[1]["response_id"]
    query_state = await get_query_state(query_id, logger)

    if query_state is None:
        logger.error(f"Query id {query_id} not found in db.")
    else:
        callback_url = query_state[8]
        if callback_url is not None:
            # this was an async query, need to send message back
            message = await get_message(response_id, logger)
            logs = await get_logs(response_id, logger)
            message["logs"] = logs
            try:
                async with httpx.AsyncClient(timeout=60) as client:
                    response = await client.post(
                        callback_url,
                        json=message,
                    )
                    response.raise_for_status()
                    logger.info(f"Sent response back to {callback_url}")
            except Exception as e:
                logger.error(f"Failed to send callback to {callback_url}: {e}")

        await set_query_completed(query_id, "OK", logger)

    await mark_task_as_complete(STREAM, GROUP, task[0], logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await finish_query(task, logger)
    finally:
        span.end()
        limiter.release()


async def poll_for_tasks():
    async for task, parent_ctx, logger, limiter in get_tasks(
        STREAM, GROUP, CONSUMER, TASK_LIMIT
    ):
        asyncio.create_task(process_task(task, parent_ctx, logger, limiter))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
