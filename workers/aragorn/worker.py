"""Aragorn ARA module."""

import asyncio
import json
import logging
import uuid

from shepherd_utils.db import get_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import (
    examine_query,
    get_tasks,
    run_task_lifecycle,
)

# Queue name
STREAM = "aragorn"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def aragorn(task, logger: logging.Logger):
    """Examine and define the Aragorn workflow."""
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(query_id, logger)
    infer, question_qnode, answer_qnode, pathfinder = examine_query(message)

    if workflow is None:
        if infer:
            workflow = [
                {"id": "aragorn.lookup"},
                {"id": "aragorn.omnicorp"},
                {"id": "aragorn.score"},
                {"id": "sort_results_score"},
                {"id": "filter_results_top_n", "parameters": {"max_results": 500}},
                {"id": "filter_kgraph_orphans"},
            ]
        elif pathfinder:
            workflow = [
                {"id": "aragorn.pathfinder"},
                {"id": "score_paths"},
                {"id": "sort_results_score"},
                {"id": "filter_analyses_top_n", "parameters": {"max_analyses": 500}},
                {"id": "filter_kgraph_orphans"},
                {"id": "aragorn.pathfinder"},
            ]
        else:
            workflow = [
                {"id": "aragorn.lookup"},
                {"id": "aragorn.omnicorp"},
                {"id": "aragorn.score"},
                {"id": "sort_results_score"},
                {"id": "filter_results_top_n", "parameters": {"max_results": 500}},
                {"id": "filter_kgraph_orphans"},
            ]

    task[1]["workflow"] = json.dumps(workflow)


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, aragorn)


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, TASK_LIMIT
            ):
                asyncio.create_task(process_task(task, parent_ctx, logger, limiter))
        except asyncio.CancelledError:
            logging.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            logging.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
