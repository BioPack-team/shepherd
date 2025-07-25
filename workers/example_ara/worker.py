"""Example ARA module."""

import asyncio
import logging

import time
import uuid
from shepherd_utils.db import get_message
from shepherd_utils.shared import get_tasks, wrap_up_task
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "example"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
tracer = setup_tracer(STREAM)


async def example_ara(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    message = await get_message(task[1]["query_id"], logger)
    # logger.info(message)
    logger.info(task)

    workflow = [
        {"id": "example.lookup"},
        {"id": "example.score"},
        {"id": "sort_results_score"},
        {"id": "filter_results_top_n"},
        {"id": "filter_kgraph_orphans"},
    ]

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)

    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await example_ara(task, logger)
    finally:
        span.end()


async def poll_for_tasks():
    async for task, parent_ctx, logger in get_tasks(STREAM, GROUP, CONSUMER):
        asyncio.create_task(process_task(task, parent_ctx, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
