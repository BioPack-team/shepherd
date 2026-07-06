"""
ARAX Rank Worker for Shepherd.

This worker implements ARAX ranking algorithm as a Shepherd worker.
It listens on the 'arax.rank' stream and ranks TRAPI message results
using the ARAX ranking algorithms (using max flow, longest path, Frobenius norm).

The ranking algorithm is a direct replication of RTX/code/ARAX/ARAXQuery/ARAX_ranker.py,
adapted for Shepherd's dict-based TRAPI message structure.

"""

import asyncio
import json
import logging
import os
import uuid
from concurrent.futures import ProcessPoolExecutor

from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, run_task_lifecycle

from ranker import arax_rank

# Queue name
STREAM = "arax.rank"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)


def rank_message(in_message: dict, logger: logging.Logger) -> dict:
    """
    Rank a TRAPI message using ARAX algorithms.

    This function is designed to be run in a process pool executor
    for CPU-intensive ranking operations.

    Args:
        in_message: TRAPI message dict
        logger: Logger instance

    Returns:
        Ranked message
    """
    # save the logs for the response (if any)
    if "logs" not in in_message or in_message["logs"] is None:
        in_message["logs"] = []
    else:
        # Convert timestamps to strings for JSON serialization
        for log in in_message.get("logs", []):
            if "timestamp" in log:
                log["timestamp"] = str(log["timestamp"])

    # Check if message has results to rank
    if not in_message.get("message"):
        logger.warning("No message found in input")
        return in_message

    msg = in_message["message"]

    if not msg.get("results"):
        logger.info("No results to rank")
        return in_message

    try:
        # Run ARAX ranking
        ranked_message = arax_rank(in_message, logger)
        logger.info(f"Successfully ranked {len(msg.get('results', []))} results")
        return ranked_message

    except Exception as e:
        logger.exception(f"ARAX ranking failed: {e}")
        # Return original message on failure
        return in_message


async def process_task(task, parent_ctx, logger, limiter, loop, executor):
    """Process a given task and ACK in redis.

    Ranking is CPU-bound, so it is dispatched to a process pool while the
    span, wrap-up, and error handling are shared with every worker.
    """

    async def _run(task, logger):
        response_id = task[1]["response_id"]
        message = await get_message(response_id, logger)
        if message is not None:
            # Run ranking in process pool for CPU-intensive operations
            ranked_message = await loop.run_in_executor(
                executor,
                rank_message,
                message,
                logger,
            )
            if ranked_message is None:
                logger.error("Ranking returned None. Returning original message.")
                ranked_message = message
            await save_message(response_id, ranked_message, logger)
        else:
            logger.error(f"Failed to get {response_id} for ranking.")

    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, _run)


async def poll_for_tasks() -> None:
    """
    Main loop to poll for and process ranking tasks.

    Creates a single ProcessPoolExecutor that is reused across all tasks
    for better performance.
    """
    loop = asyncio.get_running_loop()
    cpu_count = os.cpu_count()
    cpu_count = cpu_count if cpu_count is not None else 1
    cpu_count = min(cpu_count, TASK_LIMIT)
    executor = ProcessPoolExecutor(max_workers=cpu_count)

    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, TASK_LIMIT
            ):
                await process_task(task, parent_ctx, logger, limiter, loop, executor)
        except asyncio.CancelledError:
            logging.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            logging.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
