"""Example ARA module."""

import asyncio
import json
import logging
import time
import uuid
from shepherd_utils.db import get_message, save_callback_response, get_query_state
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "sort_results_score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


async def sort_results_score(task, logger: logging.Logger):
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
    aord = current_op.get("ascending_or_descending", "descending")
    reverse = aord == "descending"
    try:
        message["message"]["results"] = sorted(
            results,
            key=lambda x: max([y.get("score", 0) for y in x["analyses"]]),
            reverse=reverse,
        )
    except KeyError as e:
        # can't find the right structure of message
        logger.error(f"Error sorting results: {e}")
        return message, 400
    logger.info("Returning sorted results.")

    # save merged message back to db
    await save_callback_response(response_id, message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def poll_for_tasks():
    async for task, logger in get_tasks(STREAM, GROUP, CONSUMER):
        asyncio.create_task(sort_results_score(task, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
