"""Example ARA module."""
import asyncio
import logging
import json
import uuid
from shepherd_utils.broker import get_task, mark_task_as_complete, add_task
from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.db import get_message, initialize_db

setup_logging()

# Queue name
STREAM = "example"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


async def example_ara(task, logger):
    # given a task, get the message from the db
    message = await get_message(task[1]["query_id"])
    logger.info(message)

    workflow = [
        {"id": "example.lookup"},
        {"id": "example.score"},
        {"id": "sort_results_score"},
        # {"id": "filter_results_top_n"},
        # {"id": "filter_kgraph_orphans"},
    ]

    next_op = workflow[0]["id"]
    logger.info(f"Sending task to {next_op}")
    await add_task(next_op, {"query_id": task[1]["query_id"], "workflow": json.dumps(workflow)})
    
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
            asyncio.create_task(example_ara(ara_task, logger))
        else:
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
