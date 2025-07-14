"""Shared Shepherd Utility Functions."""

import json
import logging
from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.trace import Span
import orjson
from typing import AsyncGenerator, Dict, List, Tuple, Union

from .broker import add_task, get_task, mark_task_as_complete
from .db import initialize_db, save_logs
from .logger import QueryLogger, setup_logging

setup_logging()


def get_next_operation(
    workflow: List[Dict[str, str]],
) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """
    Get the next workflow operation from the list.

    Args:
        workflow (List[Dict[str, str]]): TRAPI workflow operation list
    """
    next_op = workflow[0]
    return next_op, workflow


async def get_tasks(
    stream: str, group: str, consumer: str
) -> AsyncGenerator[Tuple[Union[Tuple[str, str], None], Span, logging.Logger], None]:
    """Continually monitor the ara queue for tasks."""
    # Set up logger
    level_number = logging._nameToLevel["INFO"]
    log_handler = QueryLogger().log_handler
    worker_logger = logging.getLogger(f"shepherd.{stream}.{consumer}")
    worker_logger.setLevel(level_number)
    worker_logger.addHandler(log_handler)
    # initialize opens the db connection
    await initialize_db()
    # continuously poll the broker for new tasks
    while True:
        # get a new task for the given target
        ara_task = await get_task(stream, group, consumer, worker_logger)
        if ara_task is not None:
            log_handler = QueryLogger().log_handler
            task_logger = logging.getLogger(
                f"shepherd.{stream}.{consumer}.{ara_task[1]['query_id']}"
            )
            task_logger.setLevel(level_number)
            task_logger.addHandler(log_handler)
            task_logger.info(f"Doing task {ara_task}")
            tracer = trace.get_tracer(__name__)
            context = extract(orjson.loads(ara_task[1].get("otel", "{}")))
            otel = tracer.start_span(stream, context=context)
            # send the task to a async background task
            # this could be async, multi-threaded, etc.
            yield ara_task, otel, task_logger


async def wrap_up_task(
    stream: str,
    group: str,
    task: Tuple[str, dict],
    workflow: List[dict],
    otel: Span,
    logger: logging.Logger,
):
    """Call the next task and mark this one as complete."""
    # remove the operation we just did
    if stream == workflow[0]["id"]:
        # make sure the worker is in the workflow
        # for entry workers, they won't match and we'll do the first operation
        workflow.pop(0)
    # grab the next operation in the list
    if len(workflow) > 0:
        next_op = workflow[0]["id"]
    else:
        next_op = "finish_query"
    logger.info(f"Sending task to {next_op}")
    await add_task(
        next_op,
        {
            "query_id": task[1]["query_id"],
            "response_id": task[1]["response_id"],
            "workflow": json.dumps(workflow),
        },
        logger,
    )

    await mark_task_as_complete(stream, group, task[0], logger)
    await save_logs(task[1]["response_id"], logger)
    otel.end()
