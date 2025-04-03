"""Example ARA module."""
import asyncio
import httpx
import json
import logging
import time
from typing import List, Dict
import uuid
from shepherd_utils.broker import get_task, mark_task_as_complete, add_task
from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.db import get_message, initialize_db, add_callback_id, get_running_callbacks, save_callback_response

setup_logging()

# Queue name
STREAM = "example.lookup"
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
    if next_op_index == -1 or next_op_index > len(workflow):
        return None
    return workflow[next_op_index]


async def example_lookup(task, logger):
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(query_id)

    # Do query expansion or whatever lookup process
    # We're going to stub a response
    with open("strider_prod_response.json", "r") as f:
        response = json.load(f)
    
    async with httpx.AsyncClient(timeout=100) as client:
        for _ in range(5):
            callback_id = str(uuid.uuid4())[:8]
            # Put callback UID and query ID in postgres
            await add_callback_id(query_id, callback_id, logger)
            # put lookup query graph in redis
            await save_callback_response(f"{callback_id}_query_graph", response["message"]["query_graph"], logger)

            await client.post(
                f"http://shepherd_server:5439/callback/{callback_id}",
                json=response,
            )
            # Then we can retrieve all callback ids from query id to see which are still
            # being looked up
    
    # this worker might have a timeout set for if the lookups don't finish within a certain
    # amount of time
    MAX_QUERY_TIME = 300
    start_time = time.time()
    while time.time() - start_time < MAX_QUERY_TIME:
        # see if there are existing lookups going
        running_callback_ids = await get_running_callbacks(query_id, logger)
        logger.info(f"Got back {len(running_callback_ids)} running lookups")
        # if there are, continue to wait
        if len(running_callback_ids) > 0:
            await asyncio.sleep(1)
            continue
        # if there aren't, lookup is complete and we need to pass on to next workflow operation
        if len(running_callback_ids) == 0:
            break

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
            asyncio.create_task(example_lookup(ara_task, logger))
        else:
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
