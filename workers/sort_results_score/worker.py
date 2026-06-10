"""Example ARA module."""

import asyncio
import json
import logging
import uuid
from shepherd_utils.db import get_message, save_message, get_query_state
from shepherd_utils.shared import get_tasks, run_task_lifecycle
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "sort_results_score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def sort_results_score(task, logger: logging.Logger):
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(response_id, logger)
    results = message["message"].get("results", [])
    current_op = workflow[0]
    aord = current_op.get("ascending_or_descending", "descending")
    reverse = aord == "descending"
    for ind, result in enumerate(results):
        message["message"]["results"][ind]["analyses"] = sorted(
            result["analyses"],
            key=lambda x: x.get("score", 0),
            reverse=reverse,
        )
    if reverse:
        message["message"]["results"] = sorted(
            results,
            key=lambda x: x["analyses"][0].get("score", 0) if x["analyses"] else 0,
            reverse=reverse,
        )
    else:
        message["message"]["results"] = sorted(
            results,
            key=lambda x: x["analyses"][-1].get("score", 0) if x["analyses"] else 0,
            reverse=reverse,
        )
    logger.info("Returning sorted results.")

    # save merged message back to db
    await save_message(response_id, message, logger)


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
            logging.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            logging.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
