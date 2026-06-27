"""Node annotation worker.

Attaches Biothings annotations (``settings.annotator_url``) to every
knowledge-graph node as a ``biothings_annotations`` attribute. Runs after
normalization so annotations attach to canonical curies. Faithful to Relay's
``annotate_nodes``: on any annotator failure the message passes through
unchanged.
"""

import asyncio
import logging
import re
import uuid

import httpx

from shepherd_utils.config import settings
from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, run_task_lifecycle

# Queue name
STREAM = "node_annotate"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)

BATCH_SIZE = 1000
# Valid CURIE shape (prefix:reference); skip anything that doesn't match before
# sending to the annotator (Relay does the same).
CURIE_RE = re.compile(r"[\w\.]+:[\w\.]+")
ANNOTATION_ATTRIBUTE_TYPE = "biothings_annotations"


async def get_annotations(
    curies: list[str], logger: logging.Logger
) -> dict[str, object]:
    """Return a ``curie -> annotation`` map, or empty on failure."""
    annotations: dict[str, object] = {}
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            for start in range(0, len(curies), BATCH_SIZE):
                chunk = curies[start : start + BATCH_SIZE]
                response = await client.post(
                    settings.annotator_url, json={"ids": chunk}
                )
                response.raise_for_status()
                annotations.update(response.json() or {})
    except Exception as e:
        logger.error(f"Node annotation request failed; passing through: {e}")
        return {}
    return annotations


def annotate_message(message: dict, annotations: dict[str, object]):
    """Attach annotations to nodes as a biothings_annotations attribute in place."""
    nodes = (message.get("message", {}).get("knowledge_graph", {}) or {}).get(
        "nodes", {}
    ) or {}
    annotated = 0
    for curie, node in nodes.items():
        annotation = annotations.get(curie)
        if annotation is None:
            continue
        attributes = node.get("attributes")
        if attributes is None:
            attributes = []
            node["attributes"] = attributes
        attributes.append(
            {
                "attribute_type_id": ANNOTATION_ATTRIBUTE_TYPE,
                "value": annotation,
            }
        )
        annotated += 1
    return annotated


async def node_annotate(task, logger: logging.Logger):
    """Annotate all knowledge-graph nodes with Biothings data."""
    response_id = task[1]["response_id"]
    message = await get_message(response_id, logger)
    nodes = (message.get("message", {}).get("knowledge_graph", {}) or {}).get(
        "nodes", {}
    ) or {}
    curies = [c for c in nodes.keys() if CURIE_RE.fullmatch(c)]
    if not curies:
        logger.info("No annotatable nodes.")
        return
    annotations = await get_annotations(curies, logger)
    if not annotations:
        logger.info("Annotator returned nothing; leaving message unchanged.")
        return
    annotated = annotate_message(message, annotations)
    logger.info(f"Annotated {annotated}/{len(curies)} nodes.")
    await save_message(response_id, message, logger)


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, node_annotate
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
