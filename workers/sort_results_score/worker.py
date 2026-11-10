"""Sort results based on score worker."""

import asyncio
import logging
import uuid
from shepherd_utils.db import (
    get_message,
    save_response,
    get_query_state,
)
from shepherd_utils.shared import get_tasks, run_task_lifecycle
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.local_ops import apply_local_steps
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "sort_results_score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 3
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)


async def sort_results_score(task, logger: logging.Logger):
    """Sort the results by score, then run any local steps that follow
    (shepherd_utils.local_ops) on the same loaded message."""
    response_id = task[1]["response_id"]
    # The response-size guard runs for every worker in run_task_lifecycle,
    # before the worker function is called.
    message = await get_message(response_id, logger)
    apply_local_steps(STREAM, task, message, logger)
    await save_response(response_id, message, logger)


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, sort_results_score
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
            LOGGER.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
