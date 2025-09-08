"""SIPR (Set-Input Page Rank) module."""

import asyncio
import logging
import time
import uuid

from shepherd_utils.db import get_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "sipr"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def sipr(task, logger: logging.Logger):
    start = time.time()
    workflow = []
    try:
        # given a task, get the message from the db
        logger.info("Getting message from db")
        message = await get_message(task[1]["query_id"], logger)

        # graph retrieval

        # distribute weights

        # prune

        # return results

    except Exception as e:
        logger.error(f"Something bad happened! {e}")
        # TODO: gracefully handle worker errors

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)

    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await sipr(task, logger)
    except Exception as e:
        logger.error(f"Something went wrong: {e}")
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
