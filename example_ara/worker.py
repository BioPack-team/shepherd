"""Aragorn ARA module."""
import asyncio
import logging
from shepherd_utils.broker import get_ara_task, mark_task_as_complete
from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.db import get_message, initialize_db

setup_logging()


async def example_ara(task, logger):
    # given a task, get the message from the db
    message = await get_message(task[1]["query_id"])
    
    await asyncio.sleep(60)
    await mark_task_as_complete("example_ara", task[0], logger)
    # logger.info(f"Finished task {task[0]}")


async def poll_redis():
    """Continually monitor the ara queue for tasks."""
    # Set up logger
    level_number = logging._nameToLevel["INFO"]
    log_handler = QueryLogger().log_handler
    logger = logging.getLogger("shepherd.example_ara_worker")
    logger.setLevel(level_number)
    logger.addHandler(log_handler)
    # initialize opens the db connection
    await initialize_db()
    # continuously poll the broker for new tasks
    while True:
        # get a new task for the given target
        ara_task = await get_ara_task("example_ara", logger)
        if ara_task is not None:
            logger.info(f"Doing task {ara_task}")
            # send the task to a async background task
            # this could be async, multi-threaded, etc.
            asyncio.create_task(example_ara(ara_task, logger))
        else:
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_redis())
