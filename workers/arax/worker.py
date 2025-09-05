"""ARAX entry module."""

import asyncio
import json
import logging
import requests
import time
import uuid
from shepherd_utils.db import get_message, save_message
from shepherd_utils.shared import get_tasks, wrap_up_task
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "arax"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def arax(task, logger: logging.Logger):
    try:
        start = time.time()
        query_id = task[1]["query_id"]
        logger.info(f"Getting message from db for query id {query_id}")
        message = await get_message(query_id, logger)
        message["submitter"] = "Shepherd"
        logger.info(f"Get the message from db {message}")

        arax_url = "https://arax.ncats.io/api/arax/v1.4/query"
        headers = {"Content-Type": "application/json"}
        response = requests.post(arax_url, json=message, headers=headers)

        logger.info(f"Status Code from ARAX response: {response.status_code}")
        result = response.json()



    except Exception as e:
        logger.error(f"Error occurred in ARAX entry module: {e}")
        result = {"status": "error", "error": str(e)}

    response_id = task[1]["response_id"]

    await save_message(response_id, result, logger)

    workflow = [{"id": "arax"}]

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)

    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await arax(task, logger)
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
