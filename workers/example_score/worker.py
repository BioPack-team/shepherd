"""Example ARA module."""

import asyncio
import json
import logging
import random
import time
import uuid
from shepherd_utils.db import get_message, save_message
from shepherd_utils.shared import get_tasks, handle_task_failure, wrap_up_task
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "example.score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def example_score(task, logger: logging.Logger):
    """Do a very random score."""
    # given a task, get the message from the db
    response_id = task[1]["response_id"]

    message = await get_message(response_id, logger)
    # give a random score to all results
    for result in message["message"].get("results", []):
        for analysis in result["analyses"]:
            analysis["score"] = random.random()

    await save_message(response_id, message, logger)


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    start = time.time()
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await example_score(task, logger)
        # Always wrap up the task to ACK it in the broker
        try:
            await wrap_up_task(STREAM, GROUP, task, logger)
        except Exception as e:
            logger.error(f"Task {task[0]}: Failed to wrap up task: {e}")
    except asyncio.CancelledError:
        logger.warning(f"Task {task[0]} was cancelled")
    except Exception as e:
        logger.error(f"Task {task[0]} failed with unhandled error: {e}", exc_info=True)
        await handle_task_failure(STREAM, GROUP, task, logger)
    finally:
        span.end()
        limiter.release()
        logger.info(f"Finished task {task[0]} in {time.time() - start}")


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
