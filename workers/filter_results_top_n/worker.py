"""Filter Results Top N Worker."""

import asyncio
import logging
import uuid
from shepherd_utils.db import get_message, save_response, get_query_state
from shepherd_utils.shared import run_task_lifecycle, run_poll_loop
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.local_ops import apply_local_steps
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "filter_results_top_n"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 5
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)


async def filter_results_top_n(task, logger: logging.Logger):
    """Keep the top results, then run any local steps that follow
    (shepherd_utils.local_ops) on the same loaded message."""
    response_id = task[1]["response_id"]
    message = await get_message(response_id, logger)
    apply_local_steps(STREAM, task, message, logger)
    await save_response(response_id, message, logger)


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, filter_results_top_n
    )


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    await run_poll_loop(STREAM, GROUP, CONSUMER, TASK_LIMIT, process_task)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
