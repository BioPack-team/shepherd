"""ARAX entry module."""

import asyncio
import json
import logging
import uuid

import httpx
from inject_shepherd_arax_provenance import add_shepherd_arax_to_edge_sources

from shepherd_utils.config import settings
from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, run_task_lifecycle

# Queue name
STREAM = "arax"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def arax(task, logger: logging.Logger):
    try:
        query_id = task[1]["query_id"]
        logger.info(f"Getting message from db for query id {query_id}")
        message = await get_message(query_id, logger)
        message["submitter"] = "Shepherd"
        logger.info(f"Get the message from db {message}")

        headers = {"Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=270) as client:
            response = await client.post(
                settings.arax_url, json=message, headers=headers
            )

        logger.info(f"Status Code from ARAX response: {response.status_code}")
        result = response.json()
        result = add_shepherd_arax_to_edge_sources(result)

    except Exception as e:
        logger.error(f"Error occurred in ARAX entry module: {e}")
        result = {"status": "error", "error": str(e)}

    response_id = task[1]["response_id"]

    await save_message(response_id, result, logger)

    task[1]["workflow"] = json.dumps([{"id": "arax"}])


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, arax)


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, TASK_LIMIT
            ):
                asyncio.create_task(process_task(task, parent_ctx, logger, limiter))
        except asyncio.CancelledError:
            logging.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            logging.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
