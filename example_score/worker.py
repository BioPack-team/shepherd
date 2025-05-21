"""Example ARA module."""
import asyncio
import json
import logging
import random
import time
import uuid
from shepherd_utils.broker import mark_task_as_complete, add_task
from shepherd_utils.db import get_message, get_query_state, save_callback_response
from shepherd_utils.shared import task, get_next_operation

# Queue name
STREAM = "example.score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


@task(STREAM, GROUP, CONSUMER)
async def example_score(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])

    query_state = await get_query_state(query_id, logger)
    logger.info(query_state)
    response_id = query_state[7]
    message = await get_message(response_id, logger)
    # give a random score to all results
    for result in message["message"]["results"]:
        for analysis in result["analyses"]:
            analysis["score"] = random.random()

    await save_callback_response(response_id, message, logger)
    next_op = get_next_operation(STREAM, workflow)
    if next_op is None:
        await add_task("finish_query", {"query_id": query_id}, logger)
    else:
        await add_task(next_op["id"], {"query_id": query_id, "workflow": json.dumps(workflow)}, logger)

    await mark_task_as_complete(STREAM, GROUP, task[0], logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


if __name__ == "__main__":
    asyncio.run(example_score())
