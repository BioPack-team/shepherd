"""Example ARA module."""
import asyncio
import json
import logging
from typing import List, Dict
import uuid
from shepherd_utils.broker import get_task, mark_task_as_complete, add_task
from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.db import get_message, initialize_db, save_callback_response, get_query_state

setup_logging()

# Queue name
STREAM = "sort_results_score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


def get_current_operation(current_op: str, workflow: List[Dict[str, str]]):
    """
    Get the next workflow operation from the list.
    
    Args:
        current_op (string): operation of the current worker
        workflow (List[Dict[str, str]]): TRAPI workflow operation list
    """
    cur_op_index = -1
    for index, operation in enumerate(workflow):
        if operation["id"] == current_op:
            cur_op_index = index
    if cur_op_index == -1 or cur_op_index > len(workflow):
        return None
    return workflow[cur_op_index]


def get_next_operation(current_op: str, workflow: List[Dict[str, str]]):
    """
    Get the next workflow operation from the list.
    
    Args:
        current_op (string): operation of the current worker
        workflow (List[Dict[str, str]]): TRAPI workflow operation list
    """
    next_op_index = -1
    for index, operation in enumerate(workflow):
        if operation["id"] == current_op:
            next_op_index = index + 1
    if next_op_index == -1 or next_op_index >= len(workflow):
        return None
    return workflow[next_op_index]


async def sort_results_score(task, logger: logging.Logger):
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    query_state = await get_query_state(query_id, logger)
    response_id = query_state[7]
    message = await get_message(response_id)
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
        await add_task("finish_query", {"query_id": query_id})
    else:
        await add_task(next_op["id"], {"query_id": query_id, "workflow": json.dumps(workflow)})
    
    await mark_task_as_complete(STREAM, GROUP, task[0], logger)
    # logger.info(f"Finished task {task[0]}")


async def poll_for_tasks():
    """Continually monitor the ara queue for tasks."""
    # Set up logger
    level_number = logging._nameToLevel["INFO"]
    log_handler = QueryLogger().log_handler
    logger = logging.getLogger(f"shepherd.{STREAM}.{CONSUMER}")
    logger.setLevel(level_number)
    logger.addHandler(log_handler)
    # initialize opens the db connection
    await initialize_db()
    # continuously poll the broker for new tasks
    while True:
        # get a new task for the given target
        ara_task = await get_task(STREAM, GROUP, CONSUMER, logger)
        if ara_task is not None:
            logger.info(f"Doing task {ara_task}")
            # send the task to a async background task
            # this could be async, multi-threaded, etc.
            asyncio.create_task(sort_results_score(ara_task, logger))
        else:
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
