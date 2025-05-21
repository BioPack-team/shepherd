"""Aragorn ARA scoring module."""
import asyncio
import httpx
import json
import logging
import uuid
from shepherd_utils.broker import mark_task_as_complete, add_task
from shepherd_utils.db import get_message, save_callback_response
from shepherd_utils.shared import task, get_next_operation

# Queue name
STREAM = "aragorn.score"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


@task(STREAM, GROUP, CONSUMER)
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

        await save_callback_response(query_id, scored_message, logger)
    
    next_op = get_next_operation(STREAM, workflow)
    if next_op is None:
        await add_task("finish_query", {"query_id": query_id}, logger)
    else:
        await add_task(next_op["id"], {"query_id": query_id, "workflow": json.dumps(workflow)}, logger)
    
    await mark_task_as_complete(STREAM, GROUP, task[0], logger)


if __name__ == "__main__":
    asyncio.run(aragorn_score())
