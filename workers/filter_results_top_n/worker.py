"""Example ARA module."""

import asyncio
import json
import logging
import time
import uuid
from shepherd_utils.db import get_message, save_message, get_query_state
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "filter_results_top_n"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


async def filter_results_top_n(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    query_state = await get_query_state(query_id, logger)
    response_id = query_state[7]
    message = await get_message(response_id, logger)
    results = message["message"].get("results", [])
    current_op = workflow[0]
    if current_op is None:
        logger.error(f"Unable to find operation {STREAM} in workflow")
        raise Exception(f"Operation {STREAM} is not in workflow")
    n = current_op.get("max_results", 500)
    try:
        message["message"]["results"] = results[:n]
    except KeyError as e:
        # can't find the right structure of message
        logger.error(f"Error filtering results: {e}")
        return message, 400
    logger.info("Returning filtered results.")

    # save merged message back to db
    await save_message(response_id, message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def poll_for_tasks():
    async for task, logger in get_tasks(STREAM, GROUP, CONSUMER):
        asyncio.create_task(filter_results_top_n(task, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
