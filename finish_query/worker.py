"""Mark a query as completed and do any callbacks."""
import asyncio
import httpx
import logging
import uuid
from shepherd_utils.broker import get_task, mark_task_as_complete
from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.db import get_message, initialize_db, get_query_state, set_query_completed

setup_logging()

# Queue name
STREAM = "finish_query"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


async def finish_query(task, logger):
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    query_state = await get_query_state(query_id, logger)

    if query_state is None:
        logger.error(f"Query id {query_id} not found in db.")
    else:
        callback_url = query_state[8]
        if callback_url is not None:
            # this was an async query, need to send message back
            message = await get_message(query_state.response_id)
            async with httpx.AsyncClient() as client:
                await client.post(
                    callback_url,
                    json=message,
                )
        
        await set_query_completed(query_id, "OK", logger)

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
            asyncio.create_task(finish_query(ara_task, logger))
        else:
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
