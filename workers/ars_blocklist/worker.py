"""Blocklist filtering worker.

Drops knowledge-graph edges that come from a blocked knowledge source or touch a
blocked node (``settings.ars_blocklist_path``), removes blocked nodes, then runs
``filter_kgraph_orphans`` to clean up dangling nodes/edges/aux-graphs and prune
results that no longer bind anything. Faithful to Relay's ``remove_blocked``.
"""

import asyncio
import json
import logging
import os
import uuid

from shepherd_utils.config import settings
from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import filter_kgraph_orphans, get_tasks, run_task_lifecycle

# Queue name
STREAM = "ars_blocklist"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


def load_blocklist(path: str, logger: logging.Logger) -> tuple[set, set]:
    """Load ``(blocked_sources, blocked_nodes)`` from a JSON file.

    The file may be a list of node curies (Relay style) or an object with
    ``knowledge_sources`` and/or ``nodes`` keys. Missing/invalid files yield
    empty sets (feature effectively disabled).
    """
    if not path or not os.path.exists(path):
        return set(), set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load blocklist {path}: {e}")
        return set(), set()
    if isinstance(data, list):
        return set(), set(data)
    if isinstance(data, dict):
        return set(data.get("knowledge_sources") or []), set(data.get("nodes") or [])
    return set(), set()


# Loaded once per worker process; the blocklist is small and static.
BLOCKED_SOURCES, BLOCKED_NODES = load_blocklist(
    settings.ars_blocklist_path, logging.getLogger(STREAM)
)


def _edge_is_blocked(edge: dict, blocked_sources: set, blocked_nodes: set) -> bool:
    if edge.get("subject") in blocked_nodes or edge.get("object") in blocked_nodes:
        return True
    for source in edge.get("sources", []) or []:
        if source.get("resource_id") in blocked_sources:
            return True
    return False


def apply_blocklist(
    message: dict,
    blocked_sources: set,
    blocked_nodes: set,
    logger: logging.Logger,
) -> int:
    """Remove blocked edges + nodes from the kgraph in place. Returns #removed."""
    kg = message.get("message", {}).get("knowledge_graph") or {}
    edges = kg.get("edges") or {}
    nodes = kg.get("nodes") or {}

    removed_edges = [
        eid
        for eid, edge in edges.items()
        if _edge_is_blocked(edge, blocked_sources, blocked_nodes)
    ]
    for eid in removed_edges:
        del edges[eid]
    removed_nodes = [nid for nid in nodes if nid in blocked_nodes]
    for nid in removed_nodes:
        del nodes[nid]
    return len(removed_edges) + len(removed_nodes)


async def ars_blocklist(task, logger: logging.Logger):
    """Filter blocked sources/nodes out of the merged message."""
    response_id = task[1]["response_id"]
    if not BLOCKED_SOURCES and not BLOCKED_NODES:
        logger.info("Blocklist empty; nothing to filter.")
        return
    message = await get_message(response_id, logger)
    removed = apply_blocklist(message, BLOCKED_SOURCES, BLOCKED_NODES, logger)
    if removed:
        filter_kgraph_orphans(message, logger)
        logger.info(f"Removed {removed} blocked edges/nodes.")
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
