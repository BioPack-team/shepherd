"""Filter Results Top N Worker."""

import asyncio
import json
import logging
import uuid
from shepherd_utils.db import get_message, save_message, get_query_state
from shepherd_utils.shared import get_tasks, run_task_lifecycle
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "filter_results_top_n"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 5
tracer = setup_tracer(STREAM)


async def filter_results_top_n(task, logger: logging.Logger):
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(response_id, logger)
    results = message["message"].get("results", [])
    current_op = workflow[0]
    if current_op is None:
        logger.error(f"Unable to find operation {STREAM} in workflow")
        raise Exception(f"Operation {STREAM} is not in workflow")
    n = current_op.get("max_results", 500)

    # Truncate in place (del results[n:]) rather than binding a results[:n]
    # slice: the slice would allocate a second list and, worse, ``results`` would
    # keep the dropped results (and everything they reference) alive until the
    # function returns -- i.e. through the save. Deleting the tail drops those
    # references now so they can be freed before we re-encode the message.
    del results[n:]
    # Rebind so the key exists even if the response arrived without a results
    # field (get returned the default []); this is a rebind, not a copy.
    message["message"]["results"] = results
    logger.info("Returning filtered results.")

    # save merged message back to db
    await save_message(response_id, message, logger)


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, filter_results_top_n
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
