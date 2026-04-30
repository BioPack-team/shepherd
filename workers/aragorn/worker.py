"""Aragorn ARA module."""

import asyncio
import json
import logging
import time
import uuid

from shepherd_utils.db import get_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, handle_task_failure, wrap_up_task

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
    except KeyError:
        qedges = {}
    try:
        # this can still fail if the input looks like e.g.:
        #  "query_graph": None
        qpaths = message.get("message", {}).get("query_graph", {}).get("paths", {})
    except KeyError:
        qpaths = {}
    if len(qpaths) > 1:
        raise Exception("Only a single path is supported")
    if (len(qpaths) > 0) and (len(qedges) > 0):
        raise Exception("Mixed mode pathfinder queries are not supported")
    pathfinder = len(qpaths) == 1
    n_infer_edges = 0
    for edge_id in qedges:
        if qedges.get(edge_id, {}).get("knowledge_type", "lookup") == "inferred":
            n_infer_edges += 1
    if n_infer_edges > 1 and n_infer_edges:
        raise Exception("Only a single infer edge is supported")
    if (n_infer_edges > 0) and (n_infer_edges < len(qedges)):
        raise Exception("Mixed infer and lookup queries not supported")
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
        raise Exception("Both nodes of creative edge pinned")
    if question_node is None:
        raise Exception("No nodes of creative edge pinned")
    return infer, question_node, answer_node, pathfinder


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
                # {"id": "aragorn.omnicorp"},
                # {"id": "aragorn.score"},
                {"id": "score_paths"},
                {"id": "sort_results_score"},
                {"id": "filter_analyses_top_n", "parameters": {"max_analyses": 500}},
                {"id": "filter_kgraph_orphans"},
                {"id": "gandalf.rehydrate"},
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
    start = time.time()
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await aragorn(task, logger)
        try:
            await wrap_up_task(STREAM, GROUP, task, logger)
        except Exception as e:
            logger.error(f"Task {task[0]}: Failed to wrap up task: {e}")
    except asyncio.CancelledError:
        logger.warning(f"Task {task[0]} was cancelled.")
    except Exception as e:
        logger.error(f"Task {task[0]} failed with unhandled error: {e}", exc_info=True)
        await handle_task_failure(STREAM, GROUP, task, logger)
    finally:
        span.end()
        limiter.release()
        logger.info(f"Task took {time.time() - start}")


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
