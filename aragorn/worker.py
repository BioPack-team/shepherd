"""Aragorn ARA module."""
import asyncio
import copy
import json
import logging
from pathlib import Path
from string import Template
import time
import uuid
from shepherd_utils.broker import get_task, mark_task_as_complete, add_task
from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.db import get_message, initialize_db

setup_logging()

# Queue name
STREAM = "aragorn"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


def examine_query(message):
    """Decides whether the input is an infer. Returns the grouping node"""
    # Currently, we support:
    # queries that are any shape with all lookup edges
    # OR
    # A 1-hop infer query.
    # OR
    # Pathfinder query
    try:
        # this can still fail if the input looks like e.g.:
        #  "query_graph": None
        qedges = message.get("message", {}).get("query_graph", {}).get("edges", {})
    except:
        qedges = {}
    n_infer_edges = 0
    for edge_id in qedges:
        if qedges.get(edge_id, {}).get("knowledge_type", "lookup") == "inferred":
            n_infer_edges += 1
    pathfinder = n_infer_edges == 3
    if n_infer_edges > 1 and n_infer_edges and not pathfinder:
        raise Exception("Only a single infer edge is supported", 400)
    if (n_infer_edges > 0) and (n_infer_edges < len(qedges)):
        raise Exception("Mixed infer and lookup queries not supported", 400)
    infer = n_infer_edges == 1
    if not infer:
        return infer, None, None, pathfinder
    qnodes = message.get("message", {}).get("query_graph", {}).get("nodes", {})
    question_node = None
    answer_node = None
    for qnode_id, qnode in qnodes.items():
        if qnode.get("ids", None) is None:
            answer_node = qnode_id
        else:
            question_node = qnode_id
    if answer_node is None:
        raise Exception("Both nodes of creative edge pinned", 400)
    if question_node is None:
        raise Exception("No nodes of creative edge pinned", 400)
    return infer, question_node, answer_node, pathfinder


async def aragorn(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    message = await get_message(query_id, logger)
    try:
        infer, question_qnode, answer_qnode, pathfinder = examine_query(message)
    except Exception as e:
        logger.error(e)
        return None, 500
    
    supported_workflow_operations = set(["lookup", "enrich_results", "overlay_connect_knodes", "score", "sort_results_score", "filter_results_top_n", "filter_kgraph_ophans"])
    workflow = None
    if "workflow" in message:
        workflow = message["workflow"]
        for workflow_op in workflow:
            if workflow_op not in supported_workflow_operations:
                logger.error(f"Unsupported workflow operation: {workflow_op}")
    else:
        if infer:
            workflow = [
                {"id": "aragorn.lookup"},
                # {"id": "aragorn.overlay_connect_knodes"},
                {"id": "aragorn.score"},
                {"id": "sort_results_score"},
                {"id": "filter_results_top_n", "parameters": {"max_results": 500}},
                {"id": "filter_kgraph_orphans"},
            ]
        elif pathfinder:
            workflow = [
                {"id": "aragorn.lookup"},
                # {"id": "aragorn.overlay_connect_knodes"},
                {"id": "aragorn.score"},
                {"id": "filter_kgraph_orphans"}
            ]
        else:
            workflow = [
                {"id": "aragorn.lookup"},
                # {"id": "aragorn.overlay_connect_knodes"},
                {"id": "aragorn.score"},
                {"id": "sort_results_score"},
                {"id": "filter_results_top_n", "parameters": {"max_results": 500}},
                {"id": "filter_kgraph_orphans"},
            ]
    
    next_op = workflow[0]["id"]
    await add_task(next_op, {"query_id": query_id, "workflow": json.dumps(workflow)}, logger)
    
    await mark_task_as_complete(STREAM, GROUP, task[0], logger)
    logger.info(f"Sending task to {next_op} and took {time.time() - start}")


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
        # logger.info("trying to get aragorn tasks")
        # get a new task for the given target
        ara_task = await get_task(STREAM, GROUP, CONSUMER, logger)
        if ara_task is not None:
            logger.info(f"Doing task {ara_task}")
            # send the task to a async background task
            # this could be async, multi-threaded, etc.
            asyncio.create_task(aragorn(ara_task, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
