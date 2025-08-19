"""Aragorn ARA module."""

import asyncio
import logging
import time
import uuid
from shepherd_utils.db import get_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "aragorn"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


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

    supported_workflow_operations = set(
        [
            "lookup",
            "enrich_results",
            "overlay_connect_knodes",
            "score",
            "sort_results_score",
            "filter_results_top_n",
            "filter_kgraph_ophans",
        ]
    )
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
                {"id": "aragorn.omnicorp"},
                {"id": "aragorn.score"},
                {"id": "sort_results_score"},
                {"id": "filter_results_top_n", "parameters": {"max_results": 500}},
                {"id": "filter_kgraph_orphans"},
            ]
        elif pathfinder:
            workflow = [
                {"id": "aragorn.lookup"},
                {"id": "aragorn.omnicorp"},
                {"id": "aragorn.score"},
                {"id": "filter_kgraph_orphans"},
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

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Task took {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await aragorn(task, logger)
    finally:
        span.end()
        limiter.release()


async def poll_for_tasks():
    async for task, parent_ctx, logger, limiter in get_tasks(STREAM, GROUP, CONSUMER, TASK_LIMIT):
        asyncio.create_task(process_task(task, parent_ctx, logger, limiter))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
