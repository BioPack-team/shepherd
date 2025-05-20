"""Aragorn ARA module."""
import asyncio
import copy
import httpx
import json
import logging
from pathlib import Path
from string import Template
import time
import uuid
from shepherd_utils.broker import get_task, mark_task_as_complete, add_task
from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.db import get_message, initialize_db, get_running_callbacks, add_callback_id, save_callback_response
from shepherd_utils.shared import get_next_operation

setup_logging()

# Queue name
STREAM = "aragorn.score"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


async def aragorn_score(task, logger):
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(query_id, logger)

    # TODO: make http request to aragorn scorer
    async with httpx.AsyncClient(timeout=100) as client:
        response = await client.post(
            "https://aragorn-ranker.renci.org/score",
            json=message,
        )
        logger.info(f"Got back {response.status_code} from Aragorn Ranker")
        response.raise_for_status()
        scored_message = response.json()

        await save_callback_response(query_id, scored_message, logger)
    
    next_op = get_next_operation(STREAM, workflow)
    if next_op is None:
        await add_task("finish_query", {"query_id": query_id}, logger)
    else:
        await add_task(next_op["id"], {"query_id": query_id, "workflow": json.dumps(workflow)}, logger)
    
    await mark_task_as_complete(STREAM, GROUP, task[0], logger)


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
        # logger.info("trying to get aragorn tasks")
        # get a new task for the given target
        ara_task = await get_task(STREAM, GROUP, CONSUMER, logger)
        if ara_task is not None:
            logger.info(f"Doing task {ara_task}")
            # send the task to a async background task
            # this could be async, multi-threaded, etc.
            asyncio.create_task(aragorn_score(ara_task, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
