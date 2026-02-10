"""Example ARA module."""

import asyncio
import json
import logging
import time
import uuid
from random import shuffle
from shepherd_utils.db import get_message, save_message, get_query_state
from shepherd_utils.shared import get_tasks, wrap_up_task
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "score_paths"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def score_paths(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(response_id, logger)
    current_op = workflow[0]
    try:
        for ind, result in enumerate(message["message"]["results"]):
            message["message"]["results"][ind]["analyses"] = [{**analysis, "score": analysis.get("score", 0) or 0} for analysis in result["analyses"]]
    except KeyError as e:
        # can't find the right structure of message
        err = f"Error scoring paths: {e}"
        logger.error(err)
        raise KeyError(err)
    logger.info("Returning scored paths.")

    # save merged message back to db
    await save_message(response_id, message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await score_paths(task, logger)
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
