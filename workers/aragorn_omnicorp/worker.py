"""Aragorn ARA module."""

import asyncio
import httpx
import json
import logging
import time
import uuid
from shepherd_utils.config import settings
from shepherd_utils.db import get_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "aragorn.omnicorp"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def aragorn_omnicorp(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(response_id, logger)

    async with httpx.AsyncClient(timeout=100) as client:
        await client.post(
            settings.omnicorp_url,
            json=message,
        )

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Task took {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await aragorn_omnicorp(task, logger)
    finally:
        span.end()
        limiter.release()


async def poll_for_tasks():
    async for task, parent_ctx, logger, limiter in get_tasks(STREAM, GROUP, CONSUMER, TASK_LIMIT):
        asyncio.create_task(process_task(task, parent_ctx, logger, limiter))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
