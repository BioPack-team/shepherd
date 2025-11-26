"""Example ARA module."""

import asyncio
import json
import logging
import time
import uuid

from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import (
    get_tasks,
    recursive_get_auxgraph_edges,
    recursive_get_edge_support_graphs,
    wrap_up_task,
)

# Queue name
STREAM = "filter_kgraph_orphans"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def filter_kgraph_orphans(task, logger: logging.Logger):
    """
    Given a TRAPI message, remove all kgraph nodes and edges that aren't referenced
    in any results.
    """
    start = time.time()
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(response_id, logger)
    try:
        results = message.get("message", {}).get("results", [])
        message_auxgraphs = message.get("message", {}).get("auxiliary_graphs", {})
        kg_edges = (
            message.get("message", {}).get("knowledge_graph", {}).get("edges", {})
        )
        nodes = set()
        edges = set()
        auxgraphs = set()
        temp_auxgraphs = set()
        temp_edges = set()
        # 1. Result node bindings
        for result in results:
            for _, knodes in result.get("node_bindings", {}).items():
                nodes.update([k["id"] for k in knodes])
        # 2. Result.Analysis edge bindings
        for result in results:
            for analysis in result.get("analyses", []):
                for _, kedges in analysis.get("edge_bindings", {}).items():
                    temp_edges.update([k["id"] for k in kedges])
                for _, path_graphs in analysis.get('path_bindings', {}).items():
                    temp_auxgraphs.update(a["id"] for a in path_graphs)
        # 3. Result.Analysis support graphs
        for result in results:
            for analysis in result.get("analyses", []):
                for auxgraph in analysis.get("support_graphs", []):
                    temp_auxgraphs.add(auxgraph)
        # 4. Support graphs from edges in 2
        for edge in temp_edges:
            try:
                edges, auxgraphs, nodes = recursive_get_edge_support_graphs(
                    edge,
                    edges,
                    auxgraphs,
                    kg_edges,
                    message_auxgraphs,
                    nodes,
                )
            except KeyError as e:
                logger.warning(f"Failed to get edge support graph {edge}: {e}")
                continue
        # 5. For all the auxgraphs collect their edges and nodes
        for auxgraph in temp_auxgraphs:
            try:
                edges, auxgraphs, nodes = recursive_get_auxgraph_edges(
                    auxgraph,
                    edges,
                    auxgraphs,
                    kg_edges,
                    message_auxgraphs,
                    nodes,
                )
            except KeyError as e:
                logger.warning(f"Failed to get auxgraph edges {auxgraph}: {e}")
                continue

        # make sure message and knowledge graph exist
        message["message"] = message.get("message") or {}
        message["message"]["knowledge_graph"] = message["message"].get("knowledge_graph") or {
            "nodes": {},
            "edges": {},
        }
        # now remove all knowledge_graph nodes and edges that are
        # not in our nodes and edges sets.
        kg_nodes = (
            message.get("message", {}).get("knowledge_graph", {}).get("nodes", {})
        )
        message["message"]["knowledge_graph"]["nodes"] = {
            nid: ndata for nid, ndata in kg_nodes.items() if nid in nodes
        }
        kg_edges = (
            message.get("message", {}).get("knowledge_graph", {}).get("edges", {})
        )
        message["message"]["knowledge_graph"]["edges"] = {
            eid: edata for eid, edata in kg_edges.items() if eid in edges
        }
        # validate_message(message)
        message["message"]["auxiliary_graphs"] = {
            auxgraph: adata
            for auxgraph, adata in message["message"]
            .get("auxiliary_graphs", {})
            .items()
            if auxgraph in auxgraphs
        }
        # is_invalid = validate_message(message)
        # if is_invalid:
        #     before_is_invalid = validate_message(initial_message)
        #     if not before_is_invalid:
        #         with open("before_filtering_message.json", "w") as f:
        #             json.dump(initial_message, f, indent=2)
        #         with open("invalid_after_message.json", "w") as f:
        #             json.dump(message, f, indent=2)
        #     else:
        #         print("this message was bad to begin with")
        #         with open("invalid_before_message.json", "w") as f:
        #             json.dump(initial_message, f, indent=2)
    except KeyError as e:
        # can't find the right structure of message
        logger.error(f"Error filtering kgraph orphans: {e}")
        # return message, 400

    # save merged message back to db
    await save_message(response_id, message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await filter_kgraph_orphans(task, logger)
    finally:
        span.end()
        limiter.release()


async def poll_for_tasks():
    async for task, parent_ctx, logger, limiter in get_tasks(
        STREAM, GROUP, CONSUMER, TASK_LIMIT
    ):
        asyncio.create_task(process_task(task, parent_ctx, logger, limiter))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
