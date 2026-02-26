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
import time
import uuid
from concurrent.futures import ProcessPoolExecutor

from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, wrap_up_task

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

    async for task, parent_ctx, logger, limiter in get_tasks(
        STREAM, GROUP, CONSUMER, TASK_LIMIT
    ):
        span = tracer.start_span(STREAM, context=parent_ctx)
        start = time.time()

        # Get task details
        response_id = task[1]["response_id"]
        workflow = json.loads(task[1]["workflow"])

        # Get message from Redis
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

        # Pass to next operation in workflow
        await wrap_up_task(STREAM, GROUP, task, workflow, logger)

        logger.info(f"Finished task {task[0]} in {time.time() - start:.2f}s")
        span.end()
        limiter.release()


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
