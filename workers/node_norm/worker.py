"""Node normalization worker.

Canonicalizes every knowledge-graph node curie via the node normalizer
(``settings.node_norm``), merging nodes that resolve to the same canonical id and
rewriting edge subject/object and result node bindings to match. Runs after the
cross-ARA merge so each ARA's scores survive (normalization only rewrites ids).

Faithful to Relay's ``canonizeMessage``: on any normalizer failure the message is
passed through unchanged so a flaky service never drops results.
"""

import asyncio
import logging
import uuid

import httpx

from shepherd_utils.config import settings
from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import combine_unique_dicts, get_tasks, run_task_lifecycle

# Queue name
STREAM = "node_norm"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)

# Node normalizer batch size. The service handles large bodies but chunking keeps
# request size bounded (mirrors how SIPR batches curies).
BATCH_SIZE = 1000


def _normalizer_url() -> str:
    return settings.node_norm.rstrip("/") + "/get_normalized_nodes"


async def get_normalized_nodes(
    curies: list[str], logger: logging.Logger
) -> dict[str, dict]:
    """Return a ``curie -> {id, label, categories}`` canonical map.

    Returns an empty map on any failure so the caller passes the message through
    unchanged.
    """
    cmap: dict[str, dict] = {}
    url = _normalizer_url()
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            for start in range(0, len(curies), BATCH_SIZE):
                chunk = curies[start : start + BATCH_SIZE]
                response = await client.post(
                    url,
                    json={
                        "curies": chunk,
                        "conflate": True,
                        "drug_chemical_conflate": True,
                    },
                )
                response.raise_for_status()
                for curie, info in (response.json() or {}).items():
                    if not info:
                        continue
                    canonical = (info.get("id") or {}).get("identifier")
                    if not canonical:
                        continue
                    cmap[curie] = {
                        "id": canonical,
                        "label": (info.get("id") or {}).get("label"),
                        "categories": info.get("type") or [],
                    }
    except Exception as e:
        logger.error(f"Node normalization request failed; passing through: {e}")
        return {}
    return cmap


def canonize_message(message: dict, cmap: dict[str, dict], logger: logging.Logger):
    """Rewrite node ids to their canonical forms, merging duplicates in place."""
    msg = message.get("message", {}) or {}
    kg = msg.get("knowledge_graph") or {"nodes": {}, "edges": {}}
    old_nodes = kg.get("nodes") or {}

    new_nodes: dict[str, dict] = {}
    for old_id, node in old_nodes.items():
        canon = cmap.get(old_id)
        target_id = canon["id"] if canon else old_id
        if canon:
            if canon.get("label"):
                node["name"] = canon["label"]
            if canon.get("categories"):
                existing = node.get("categories") or []
                node["categories"] = list(set(existing) | set(canon["categories"]))
        existing_node = new_nodes.get(target_id)
        if existing_node is None:
            new_nodes[target_id] = node
        else:
            # Two source ids collapsed onto one canonical node: merge fields.
            ec = existing_node.get("categories") or []
            nc = node.get("categories") or []
            if ec or nc:
                existing_node["categories"] = list(set(ec) | set(nc))
            existing_node["attributes"] = combine_unique_dicts(
                existing_node.get("attributes") or [],
                node.get("attributes") or [],
                logger,
            )
            if not existing_node.get("name") and node.get("name"):
                existing_node["name"] = node["name"]
    kg["nodes"] = new_nodes

    for edge in (kg.get("edges") or {}).values():
        if edge.get("subject") in cmap:
            edge["subject"] = cmap[edge["subject"]]["id"]
        if edge.get("object") in cmap:
            edge["object"] = cmap[edge["object"]]["id"]

    for result in msg.get("results") or []:
        for _, bindings in (result.get("node_bindings") or {}).items():
            for binding in bindings:
                if binding.get("id") in cmap:
                    binding["id"] = cmap[binding["id"]]["id"]

    return message


async def node_norm(task, logger: logging.Logger):
    """Canonicalize all node curies in the accumulated message."""
    response_id = task[1]["response_id"]
    message = await get_message(response_id, logger)
    nodes = (message.get("message", {}).get("knowledge_graph", {}) or {}).get(
        "nodes", {}
    ) or {}
    curies = list(nodes.keys())
    if not curies:
        logger.info("No nodes to normalize.")
        return
    cmap = await get_normalized_nodes(curies, logger)
    if not cmap:
        logger.info("Normalizer returned nothing; leaving message unchanged.")
        return
    canonize_message(message, cmap, logger)
    logger.info(f"Normalized {len(cmap)}/{len(curies)} nodes.")
    await save_message(response_id, message, logger)


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, node_norm
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
