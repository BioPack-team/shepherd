"""Shared Shepherd Utility Functions."""

import asyncio
import json
import logging
from opentelemetry.context.context import Context
from opentelemetry.propagate import extract
from typing import AsyncGenerator, Dict, List, Tuple, Union

from .broker import add_task, get_task, mark_task_as_complete
from .config import settings
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
    stream: str,
    group: str,
    consumer: str,
    task_limit: int,
) -> AsyncGenerator[
    Tuple[Tuple[str, str], Context, logging.Logger, asyncio.Semaphore], None
]:
    """Continually monitor the ara queue for tasks."""
    # Set up logger
    level_number = logging._nameToLevel[settings.log_level]
    log_handler = QueryLogger().log_handler
    worker_logger = logging.getLogger(f"shepherd.{stream}.{consumer}")
    worker_logger.setLevel(level_number)
    worker_logger.addHandler(log_handler)
    # initialize opens the db connection
    await initialize_db()
    task_limiter = asyncio.Semaphore(task_limit)
    # continuously poll the broker for new tasks
    while True:
        # check if we can take another task
        await task_limiter.acquire()
        # get a new task for the given target
        ara_task = await get_task(stream, group, consumer, worker_logger)
        if ara_task is not None:
            log_handler = QueryLogger().log_handler
            task_logger = logging.getLogger(
                f"shepherd.{stream}.{consumer}.{ara_task[1]['query_id']}"
            )
            task_log_level = int(ara_task[1].get("log_level", level_number))
            task_logger.setLevel(task_log_level)
            task_logger.addHandler(log_handler)
            task_logger.info(f"Doing task {ara_task}")
            ctx = extract(json.loads(ara_task[1].get("otel", "{}")))
            # send the task to a async background task
            # this could be async, multi-threaded, etc.
            yield ara_task, ctx, task_logger, task_limiter
        else:
            task_limiter.release()


async def wrap_up_task(
    stream: str,
    group: str,
    task: Tuple[str, dict],
    workflow: List[dict],
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
            "log_level": task[1].get("log_level", 20),
            "otel": task[1]["otel"],
        },
        logger,
    )

    await mark_task_as_complete(stream, group, task[0], logger)
    await save_logs(task[1]["response_id"], logger)


def recursive_get_edge_support_graphs(edge: str, edges: set, auxgraphs: set, message_edges: dict, message_auxgraphs: dict, nodes: set):
    """Recursive method to find auxiliary graphs to keep when filtering. Each auxiliary
    graph then has its edges filterd."""
    edges.add(edge)
    nodes.add(message_edges[edge]["subject"])
    nodes.add(message_edges[edge]["object"])
    for attribute in message_edges.get(edge, {}).get('attributes', {}):
        if attribute.get('attribute_type_id', None) == 'biolink:support_graphs':
            for auxgraph in attribute.get('value', []):
                if auxgraph not in message_auxgraphs:
                    raise KeyError(f"auxgraph {auxgraph} not in auxiliary_graphs")
                try:
                    edges, auxgraphs, nodes = recursive_get_auxgraph_edges(auxgraph, edges, auxgraphs, message_edges, message_auxgraphs, nodes)
                except KeyError as e:
                    raise e
    return edges, auxgraphs, nodes


def recursive_get_auxgraph_edges(auxgraph: str, edges: set, auxgraphs: set, message_edges: dict, message_auxgraphs: dict, nodes: set):
    """Recursive method to find edges to keep when filtering. Each edge then
    has support graphs filtered."""
    auxgraphs.add(auxgraph)
    aux_edges = message_auxgraphs.get(auxgraph, {}).get('edges', [])
    for aux_edge in aux_edges:
        if aux_edge not in message_edges:
            raise KeyError(f"aux_edge {aux_edge} not in knowledge_graph.edges")
        try:
            edges, auxgraphs, nodes = recursive_get_edge_support_graphs(aux_edge, edges, auxgraphs, message_edges, message_auxgraphs, nodes)
        except KeyError as e:
            raise e
    return edges, auxgraphs, nodes


def validate_message(message, logger):
    """Validate a given message for missing nodes."""
    valid = True
    for edge_id, edge in message["message"]["knowledge_graph"]["edges"].items():
        try:
            # print(f"Checking {edge_id}")
            assert edge["subject"] in message["message"]["knowledge_graph"]["nodes"]
            assert edge["object"] in message["message"]["knowledge_graph"]["nodes"]
            for attribute in edge.get("attibutes", []):
                if attribute["attribute_type_id"] == "biolink:support_graphs":
                    for value in attribute["value"]:
                        if value not in message["message"].get("auxiliary_graphs", {}):
                            raise AssertionError(f"Aux graph {value} is not in the aux graphs.")
        except AssertionError as e:
            valid = False
            logger.error(f"Edge {edge_id} has issues: {e}")
    if not valid:
        with open("invalid_message.json", "w", encoding="utf-8") as f:
            json.dump(message, f, indent=2)
