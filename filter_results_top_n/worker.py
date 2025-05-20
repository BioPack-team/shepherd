"""Example ARA module."""
import asyncio
import json
import logging
import time
import uuid
from shepherd_utils.broker import mark_task_as_complete, add_task
from shepherd_utils.db import get_message, save_callback_response, get_query_state
from shepherd_utils.shared import task_decorator, get_next_operation, get_current_operation

# Queue name
STREAM = "filter_results_top_n"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


@task_decorator(STREAM, GROUP, CONSUMER)
async def filter_results_top_n(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    query_state = await get_query_state(query_id, logger)
    response_id = query_state[7]
    message = await get_message(response_id, logger)
    results = message["message"].get("results", [])
    current_op = get_current_operation(STREAM, workflow)
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
    await save_callback_response(response_id, message, logger)

    next_op = get_next_operation(STREAM, workflow)
    if next_op is None:
        await add_task("finish_query", {"query_id": query_id}, logger)
    else:
        await add_task(next_op["id"], {"query_id": query_id, "workflow": json.dumps(workflow)}, logger)
    
    await mark_task_as_complete(STREAM, GROUP, task[0], logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


if __name__ == "__main__":
    asyncio.run(filter_results_top_n())
