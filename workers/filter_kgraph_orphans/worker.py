"""Example ARA module."""

import asyncio
import json
import logging
import time
import uuid

from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import (
    filter_kgraph_orphans,
    get_tasks,
    wrap_up_task,
)

# Queue name
STREAM = "filter_kgraph_orphans"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def do_filter_kgraph_orphans(task, logger: logging.Logger):
    """
    Given a TRAPI message, remove all kgraph nodes and edges that aren't referenced
    in any results.
    """
    start = time.time()
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(response_id, logger)
    filter_kgraph_orphans(message, logger)

    # save merged message back to db
    await save_message(response_id, message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await do_filter_kgraph_orphans(task, logger)
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
