"""Example ARA module."""
import asyncio
import json
import logging
import time
import uuid
from shepherd_utils.broker import mark_task_as_complete, add_task
from shepherd_utils.db import get_message
from shepherd_utils.shared import task_decorator

# Queue name
STREAM = "example"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


@task_decorator(STREAM, GROUP, CONSUMER)
async def example_ara(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    message = await get_message(task[1]["query_id"], logger)
    # logger.info(message)

    workflow = [
        {"id": "example.lookup"},
        {"id": "example.score"},
        {"id": "sort_results_score"},
        {"id": "filter_results_top_n"},
        {"id": "filter_kgraph_orphans"},
    ]

    next_op = workflow[0]["id"]
    logger.info(f"Sending task to {next_op}")
    await add_task(next_op, {"query_id": task[1]["query_id"], "workflow": json.dumps(workflow)}, logger)
    
    await mark_task_as_complete(STREAM, GROUP, task[0], logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


if __name__ == "__main__":
    asyncio.run(example_ara())
