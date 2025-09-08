"""SIPR (Set-Input Page Rank) module."""

import asyncio
import json
import logging
import time
import uuid

import httpx

from shepherd_utils.config import settings
from shepherd_utils.db import (
    add_callback_id,
    get_message,
    get_running_callbacks,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "sipr"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
MAX_QUERY_TIME = 300
tracer = setup_tracer(STREAM)


async def sipr(task, logger: logging.Logger):
    start = time.time()
    workflow = [
        {"id": "sipr"},
    ]
    try:
        # given a task, get the message from the db
        logger.info("Getting message from db")
        query_id = task[1]["query_id"]
        message = await get_message(query_id, logger)

        # graph retrieval

        # distribute weights

        # prune

        # return results
        # Put callback UID and query ID in postgres
        callback_id = str(uuid.uuid4())[:8]
        await add_callback_id(query_id, callback_id, logger)
        # put lookup query graph in redis
        await save_message(
            f"{query_id}_lookup_query_graph", message["message"]["query_graph"], logger
        )
        message["callback"] = f"{settings.callback_host}/aragorn/callback/{callback_id}"

        async with httpx.AsyncClient(timeout=100) as client:
            await client.post(
                settings.kg_retrieval_url,
                json=message,
            )

        # this worker might have a timeout set for if the lookups don't finish within a certain
        # amount of time
        start_time = time.time()
        running_callback_ids = [""]
        while time.time() - start_time < MAX_QUERY_TIME:
            # see if there are existing lookups going
            running_callback_ids = await get_running_callbacks(query_id, logger)
            # logger.info(f"Got back {len(running_callback_ids)} running lookups")
            # if there are, continue to wait
            if len(running_callback_ids) > 0:
                await asyncio.sleep(1)
                continue
            # if there aren't, lookup is complete and we need to pass on to next workflow operation
            if len(running_callback_ids) == 0:
                logger.debug("Got all lookups back. Continuing...")
                break

        if time.time() - start_time > MAX_QUERY_TIME:
            logger.warning(
                f"Timed out getting lookup callbacks. {len(running_callback_ids)} queries still running..."
            )

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
