"""Blocklist filtering worker.

Removes blocked nodes (and everything that depends on them) from the merged
message, ported from Relay's ``remove_blocked``. The blocklist file
(``settings.ars_blocklist_path``) is a JSON object **keyed by the blocked node
CURIE** (values carry ``name``/``type`` metadata that is not used for matching).
Blocking a node cascades to: edges touching it, auxiliary graphs whose edges are
removed, edges whose ``biolink:support_graphs`` lose all their graphs, analyses
that lose all their edge bindings, and results left with no analyses.
"""

import asyncio
import json
import logging
import os
import uuid

from shepherd_utils.config import settings
from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, run_task_lifecycle

# Queue name
STREAM = "ars_blocklist"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


def load_blocklist(path: str, logger: logging.Logger) -> set:
    """Load the set of blocked node CURIEs from the blocklist file.

    The Relay format is a JSON object keyed by CURIE (``{"CURIE": {"name":...,
    "type":[...]}, ...}``); a plain list of curies is also tolerated. Missing or
    invalid files yield an empty set (feature disabled).
    """
    if not path or not os.path.exists(path):
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load blocklist {path}: {e}")
        return set()
    if isinstance(data, dict):
        return set(data.keys())
    if isinstance(data, list):
        return set(data)
    logger.error(f"Unexpected blocklist structure in {path}: {type(data)}")
    return set()


# Loaded once per worker process; the blocklist is small and static.
BLOCKED_NODES = load_blocklist(settings.ars_blocklist_path, logging.getLogger(STREAM))


def apply_blocklist(message: dict, blocked_curies: set, logger: logging.Logger) -> dict:
    """Remove blocked nodes and their dependents in place (Relay ``remove_blocked``).

    Returns a counts dict ``{nodes, edges, results, auxiliary_graphs, analyses}``.
    """
    counts = {"nodes": 0, "edges": 0, "results": 0, "auxiliary_graphs": 0, "analyses": 0}
    msg = message.get("message", {})
    kg = msg.get("knowledge_graph") or {}
    nodes = kg.get("nodes") or {}
    edges = kg.get("edges") or {}
    aux_graphs = msg.get("auxiliary_graphs") or {}
    results = msg.get("results") or []

    nodes_to_remove = set(blocked_curies) & set(nodes.keys())
    if not nodes_to_remove:
        return counts
    for nid in nodes_to_remove:
        del nodes[nid]

    # Edges whose subject or object is a blocked node.
    edges_to_remove = [
        eid
        for eid, edge in edges.items()
        if edge.get("subject") in nodes_to_remove
        or edge.get("object") in nodes_to_remove
    ]

    # Auxiliary graphs: drop the whole graph if all its edges go, else just the
    # offending edges; then edges whose support_graphs are all gone are removed too.
    aux_graphs_to_remove = []
    if aux_graphs:
        removed_edge_set = set(edges_to_remove)
        for aux_id, aux_graph in aux_graphs.items():
            aux_edges = aux_graph.get("edges") or []
            overlap = set(aux_edges) & removed_edge_set
            if aux_edges and len(overlap) == len(aux_edges):
                aux_graphs_to_remove.append(aux_id)
            for eid in overlap:
                aux_edges.remove(eid)
        for aux_id in aux_graphs_to_remove:
            del aux_graphs[aux_id]
        removed_aux_set = set(aux_graphs_to_remove)
        for eid, edge in edges.items():
            for attr in edge.get("attributes") or []:
                if attr.get("attribute_type_id") != "biolink:support_graphs":
                    continue
                value = attr.get("value") or []
                overlap = set(value) & removed_aux_set
                if not overlap:
                    continue
                for graph in overlap:
                    value.remove(graph)
                if len(value) == 0 and eid not in edges_to_remove:
                    edges_to_remove.append(eid)

    for eid in edges_to_remove:
        if eid in edges:
            del edges[eid]

    # Results: drop those binding a removed node; prune analyses whose edge
    # bindings point at removed edges; drop results left with no analyses.
    removed_edge_set = set(edges_to_remove)
    results_to_remove = []
    for result in results:
        for bindings in (result.get("node_bindings") or {}).values():
            for binding in bindings:
                if binding.get("id") in nodes_to_remove and result not in results_to_remove:
                    results_to_remove.append(result)
        analyses = result.get("analyses")
        if analyses is not None:
            analyses_to_remove = []
            for analysis in analyses:
                for _, bindings in (analysis.get("edge_bindings") or {}).items():
                    to_drop = []
                    for binding in bindings:
                        if binding.get("id") in removed_edge_set:
                            if len(bindings) > 1:
                                to_drop.append(binding)
                            elif analysis not in analyses_to_remove:
                                analyses_to_remove.append(analysis)
                    for binding in to_drop:
                        bindings.remove(binding)
                support_graphs = analysis.get("support_graphs")
                if support_graphs:
                    for sg in [s for s in support_graphs if s in removed_edge_set]:
                        support_graphs.remove(sg)
            for analysis in analyses_to_remove:
                counts["analyses"] += 1
                analyses.remove(analysis)
            if len(analyses) == 0 and result not in results_to_remove:
                results_to_remove.append(result)
    for result in results_to_remove:
        results.remove(result)

    counts.update(
        nodes=len(nodes_to_remove),
        edges=len(edges_to_remove),
        results=len(results_to_remove),
        auxiliary_graphs=len(aux_graphs_to_remove),
    )
    return counts


async def ars_blocklist(task, logger: logging.Logger):
    """Remove blocked nodes (and dependents) from the merged message."""
    response_id = task[1]["response_id"]
    if not BLOCKED_NODES:
        logger.info("Blocklist empty; nothing to filter.")
        return
    message = await get_message(response_id, logger)
    counts = apply_blocklist(message, BLOCKED_NODES, logger)
    if counts["nodes"]:
        logger.info(f"Blocklist removed {counts}.")
        await save_message(response_id, message, logger)


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, ars_blocklist
    )


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
