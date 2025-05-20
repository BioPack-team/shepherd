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
STREAM = "sort_results_score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


@task_decorator(STREAM, GROUP, CONSUMER)
async def sort_results_score(task, logger: logging.Logger):
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
    aord = current_op.get("ascending_or_descending", "descending")
    reverse = (aord == "descending")
    try:
        message["message"]["results"] = sorted(results, key=lambda x: max([y.get("score", 0) for y in x["analyses"]]), reverse=reverse)
    except KeyError as e:
        # can't find the right structure of message
        logger.error(f"Error sorting results: {e}")
        return message, 400
    logger.info("Returning sorted results.")

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
    asyncio.run(sort_results_score())
