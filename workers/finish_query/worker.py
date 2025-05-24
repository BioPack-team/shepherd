"""Mark a query as completed and do any callbacks."""

import asyncio
import httpx
import logging
import time
import uuid
from shepherd_utils.broker import mark_task_as_complete
from shepherd_utils.db import get_message, get_query_state, set_query_completed
from shepherd_utils.shared import get_tasks

# Queue name
STREAM = "finish_query"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


async def finish_query(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    query_state = await get_query_state(query_id, logger)

    if query_state is None:
        logger.error(f"Query id {query_id} not found in db.")
    else:
        callback_url = query_state[8]
        if callback_url is not None:
            # this was an async query, need to send message back
            message = await get_message(query_state.response_id, logger)
            async with httpx.AsyncClient() as client:
                await client.post(
                    callback_url,
                    json=message,
                )

        await set_query_completed(query_id, "OK", logger)

    await mark_task_as_complete(STREAM, GROUP, task[0], logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def poll_for_tasks():
    async for task, logger in get_tasks(STREAM, GROUP, CONSUMER):
        asyncio.create_task(finish_query(task, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
