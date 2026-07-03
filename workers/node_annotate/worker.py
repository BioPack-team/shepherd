"""Node annotation worker.

Attaches Biothings annotations to every knowledge-graph node as a
``biothings_annotations`` attribute. Runs after normalization so annotations
attach to canonical curies.

This uses exactly what the ARS uses: the ``biothings_annotator`` package
(``annotator.Annotator().annotate_curie_list``), not an HTTP API. Faithful to
Relay's ``annotate_nodes``: nodes with no annotation are left untouched and a
total annotator failure passes the message through unchanged.
"""

import asyncio
import logging
import re
import uuid

from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, run_task_lifecycle

# Queue name
STREAM = "node_annotate"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)

# Valid CURIE shape (prefix:reference); skip anything that doesn't match before
# sending to the annotator (Relay does the same).
CURIE_RE = re.compile(r"[\w\.]+:[\w\.]+")
ANNOTATION_ATTRIBUTE_TYPE = "biothings_annotations"


def _annotate_curie_list(curies: list[str]):
    """Return the ``biothings_annotator`` coroutine that annotates ``curies``.

    Matches Relay: ``annotator.Annotator().annotate_curie_list(curie_list)``.
    The import is local so the (git-installed) package is only required at
    runtime and this seam can be mocked in tests.
    """
    from biothings_annotator import annotator

    return annotator.Annotator().annotate_curie_list(curies)


async def get_annotations(
    curies: list[str], logger: logging.Logger
) -> dict[str, object]:
    """Return a ``curie -> annotation`` map via the biothings_annotator package.

    Returns an empty map on failure so the message passes through unchanged.
    """
    if not curies:
        return {}
    try:
        result = await _annotate_curie_list(curies)
        return dict(result) if result else {}
    except Exception as e:
        logger.error(f"Node annotation failed; passing through: {e}")
        return {}


def _is_empty_annotation(annotation) -> bool:
    """True for the annotator's "no data" markers (Relay skips these): an empty
    dict, or a list whose first entry is ``{"notfound": true}``."""
    if annotation == {} or annotation == []:
        return True
    if isinstance(annotation, list) and annotation:
        first = annotation[0]
        if isinstance(first, dict) and first.get("notfound") is True:
            return True
    return False


def annotate_message(message: dict, annotations: dict[str, object]):
    """Attach annotations to nodes as a biothings_annotations attribute in place."""
    nodes = (message.get("message", {}).get("knowledge_graph", {}) or {}).get(
        "nodes", {}
    ) or {}
    annotated = 0
    for curie, node in nodes.items():
        annotation = annotations.get(curie)
        if annotation is None or _is_empty_annotation(annotation):
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
