"""Example ARA module."""

import asyncio
import json
import logging

import uuid
from shepherd_utils.db import get_message
from shepherd_utils.shared import get_tasks, run_task_lifecycle
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "example"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def example_ara(task, logger: logging.Logger):
    # given a task, get the message from the db
    logger.info("Getting message from db")
    message = await get_message(task[1]["query_id"], logger)
    # logger.info(message)

    workflow = [
        {"id": "example.lookup"},
        {"id": "example.score"},
        {"id": "sort_results_score"},
        {"id": "filter_results_top_n"},
        {"id": "filter_kgraph_orphans"},
    ]

    task[1]["workflow"] = json.dumps(workflow)


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, example_ara
    )


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
