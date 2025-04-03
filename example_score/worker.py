"""Example ARA module."""
import asyncio
import json
import logging
import random
from typing import List, Dict
import uuid
from shepherd_utils.broker import get_task, mark_task_as_complete, add_task
from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.db import get_message, initialize_db, get_query_state, save_callback_response

setup_logging()

# Queue name
STREAM = "example.score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


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


async def example_score(task, logger):
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])

    query_state = await get_query_state(query_id, logger)
    logger.info(query_state)
    response_id = query_state[7]
    message = await get_message(response_id)
    # give a random score to all results
    for result in message["message"]["results"]:
        for analysis in result["analyses"]:
            analysis["score"] = random.random()

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
            asyncio.create_task(example_score(ara_task, logger))
        else:
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
