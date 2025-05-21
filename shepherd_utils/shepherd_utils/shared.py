"""Shared Shepherd Utility Functions."""
import asyncio
from functools import wraps
import logging
from typing import List, Dict, Callable, Any

from .logger import QueryLogger, setup_logging
from .db import initialize_db
from .broker import get_task

setup_logging()


def get_next_operation(current_op: str, workflow: List[Dict[str, str]]):
    """
    Get the next workflow operation from the list.
    
    Args:
        current_op (string): operation of the current worker
        workflow (List[Dict[str, str]]): TRAPI workflow operation list
    """
    next_op_index = -1
    for index, operation in enumerate(workflow):
        if operation["id"] == current_op:
            next_op_index = index + 1
    if next_op_index == -1 or next_op_index > len(workflow) - 1:
        return None
    return workflow[next_op_index]


def get_current_operation(current_op: str, workflow: List[Dict[str, str]]):
    """
    Get the next workflow operation from the list.
    
    Args:
        current_op (string): operation of the current worker
        workflow (List[Dict[str, str]]): TRAPI workflow operation list
    """
    cur_op_index = -1
    for index, operation in enumerate(workflow):
        if operation["id"] == current_op:
            cur_op_index = index
    if cur_op_index == -1 or cur_op_index > len(workflow):
        return None
    return workflow[cur_op_index]


def task(stream, group, consumer):
    def decorator(fcn: Callable[..., Any]) -> Callable[[], Any]:
        @wraps(fcn)
        async def wrapper():
            """Continually monitor the ara queue for tasks."""
            # Set up logger
            level_number = logging._nameToLevel["INFO"]
            log_handler = QueryLogger().log_handler
            logger = logging.getLogger(f"shepherd.{stream}.{consumer}")
            logger.setLevel(level_number)
            logger.addHandler(log_handler)
            # initialize opens the db connection
            await initialize_db()
            # continuously poll the broker for new tasks
            while True:
                # get a new task for the given target
                ara_task = await get_task(stream, group, consumer, logger)
                if ara_task is not None:
                    logger.info(f"Doing task {ara_task}")
                    # send the task to a async background task
                    # this could be async, multi-threaded, etc.
                    asyncio.create_task(fcn(ara_task, logger))
        return wrapper
    return decorator
