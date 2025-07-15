"""Example ARA module."""

import asyncio
import json
import logging
import random
import time
import uuid
from shepherd_utils.db import get_message, save_message
from shepherd_utils.shared import get_tasks, wrap_up_task
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "example.score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
tracer = setup_tracer(STREAM)


async def example_score(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])

    message = await get_message(response_id, logger)
    # give a random score to all results
    for result in message["message"]["results"]:
        for analysis in result["analyses"]:
            analysis["score"] = random.random()

    await save_message(response_id, message, logger)
    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await example_score(task, logger)
    finally:
        span.end()


async def poll_for_tasks():
    async for task, parent_ctx, logger in get_tasks(STREAM, GROUP, CONSUMER):
        asyncio.create_task(process_task(task, parent_ctx, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
