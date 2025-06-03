"""Aragorn ARA scoring module."""

import asyncio
import httpx
import json
import logging
import uuid
from shepherd_utils.db import get_message, save_message
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "aragorn.score"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


async def aragorn_score(task, logger: logging.Logger):
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

        await save_message(query_id, scored_message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)


async def poll_for_tasks():
    async for task, logger in get_tasks(STREAM, GROUP, CONSUMER):
        asyncio.create_task(aragorn_score(task, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
