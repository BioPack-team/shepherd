"""BTE ARA module."""

import asyncio
import functools
import json
import logging
import uuid
from pathlib import Path
from string import Template
from typing import Any, Dict, Optional

from pydantic import BaseModel, parse_obj_as

from shepherd_utils.config import settings
from shepherd_utils.db import get_message
from shepherd_utils.lookups import submit_lookups, wait_for_callbacks
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.query_errors import UnsupportedQueryError
from shepherd_utils.shared import get_tasks, lookup_wait_seconds, run_task_lifecycle

# Queue name
STREAM = "bte.lookup"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)


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
    n_infer_edges = 0
    for edge_id in qedges:
        if qedges.get(edge_id, {}).get("knowledge_type", "lookup") == "inferred":
            n_infer_edges += 1
    pathfinder = n_infer_edges == 3
    if n_infer_edges > 1 and n_infer_edges and not pathfinder:
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


async def bte_lookup(task, logger: logging.Logger):
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    response_id = task[1]["response_id"]
    message = await get_message(query_id, logger)
    parameters = message.get("parameters") or {}
    parameters["timeout"] = parameters.get("timeout", settings.lookup_timeout)
    parameters["tiers"] = parameters.get("tiers") or [settings.default_data_tier]
    message["parameters"] = parameters
    if "submitter" not in message:
        message["submitter"] = (
            "infores:shepherd-bte:{maturity}@{location}@{url}".format(
                maturity=settings.server_maturity,
                location=settings.server_location,
                url=settings.server_url,
            )
        )
    infer, question_qnode, answer_qnode, pathfinder = examine_query(message)
    if pathfinder:
        # BTE currently doesn't handle Pathfinder queries
        raise UnsupportedQueryError("BTE does not support Pathfinder type queries.")

    if not infer:
        # C6-3: the plain lookup goes through the same submission as the
        # expanded ones, so a rejected POST is an error rather than a silent
        # wait on a callback that never comes.
        lookups = [message]
    else:
        lookups = expand_bte_query(message, logger)
        logger.info(f"Expanded to {len(lookups)} messages")
    await submit_lookups(lookups, query_id, "bte", logger, span_name=STREAM)

    # this worker might have a timeout set for if the lookups don't finish within a certain
    # amount of time
    await wait_for_callbacks(
        query_id, response_id, lookup_wait_seconds(message["parameters"]), logger
    )


class TemplateGroup(BaseModel):
    """A group of templates to be matched by given criteria."""

    name: str
    subject: list[str]
    predicate: list[str]
    object: list[str]
    templates: list[str]
    qualifiers: Optional[dict[str, str]]


def get_params(
    query_graph: Dict,
) -> tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    dict[str, str],
]:
    """Obtain some important parameters from the query graph."""
    edge = next(iter(query_graph["edges"].values()))

    q_subject = query_graph["nodes"].get(edge["subject"])
    subject_type = next(iter(q_subject.get("categories") or []), None)

    q_object = query_graph["nodes"].get(edge["object"])
    object_type = next(iter(q_object.get("categories") or []), None)

    predicate = next(iter(edge.get("predicates") or []), None)

    subject_curie = next(iter(q_subject.get("ids") or []), None)
    object_curie = next(iter(q_object.get("ids") or []), None)
    qualifiers: dict[str, str] = {}

    # TRAPI 2.0: each qualifier set is a {qualifier_type_id: value} mapping in
    # constraints.qualifiers. Only the first set is used for template matching.
    qualifier_sets = (edge.get("constraints") or {}).get("qualifiers") or []
    if len(qualifier_sets) > 0:
        qualifiers = dict(qualifier_sets[0] or {})

    return (
        edge["subject"],
        subject_type,
        subject_curie,
        edge["object"],
        object_type,
        object_curie,
        predicate,
        qualifiers,
    )


# C9-5: the template groups, the template index and every template's text are
# read once per process. Each inferred query used to re-read and re-parse
# template_groups.json, walk the templates directory and re-read each matched
# template file.
@functools.cache
def _template_groups() -> list:
    with open(
        Path(__file__).parent / "template_groups.json", "r", encoding="utf-8"
    ) as file:
        return parse_obj_as(list[TemplateGroup], json.load(file))


@functools.cache
def _template_paths() -> dict:
    return {
        path.name: path
        for path in (Path(__file__).parent / "templates").rglob("*.json")
    }


@functools.cache
def _template(path: Path) -> Template:
    with open(path, "r", encoding="utf-8") as file:
        return Template(file.read())


def match_templates(
    subject_type: Optional[str],
    object_type: Optional[str],
    predicate: Optional[str],
    qualifiers: dict[str, str],
    logger: logging.Logger,
) -> list[Path]:
    """Match a given set of parameters to a number of templates."""

    # TODO: expand subject/object types by descending the biolink hierarchy
    subject_types: set[str] = set()
    object_types: set[str] = set()
    predicates: set[str] = set()
    if subject_type is not None:
        subject_types.add(subject_type.removeprefix("biolink:"))
    if object_type is not None:
        object_types.add(object_type.removeprefix("biolink:"))
    if predicate is not None:
        predicates.add(predicate.removeprefix("biolink:"))

    templateGroups = _template_groups()
    template_paths = _template_paths()

    matched_paths: set[Path] = set()
    for group in templateGroups:
        conditions: list[bool] = []
        conditions.append(len(subject_types.intersection(group.subject)) > 0)
        conditions.append(len(object_types.intersection(group.object)) > 0)
        conditions.append(len(predicates.intersection(group.predicate)) > 0)
        # Qualifiers (if they exist) are satisfied. template_groups.json names
        # qualifier types without the "biolink:" prefix the query uses.
        group_qualifiers = {
            qualifier_type.removeprefix("biolink:"): value
            for qualifier_type, value in (group.qualifiers or {}).items()
        }
        conditions.append(
            all(
                group_qualifiers.get(qualifier_type.removeprefix("biolink:"), False)
                == value
                for qualifier_type, value in qualifiers.items()
            )
        )

        if all(conditions):
            for template in group.templates:
                matched_paths.add(template_paths[template])

    return list(matched_paths)


def fill_templates(
    paths: list[Path],
    query_body: Dict,
    subject_key: Optional[str],
    subject_curie: Optional[str],
    object_key: Optional[str],
    object_curie: Optional[str],
) -> list[Dict]:
    filled_templates: list[Dict] = []
    for path in paths:
        template = _template(path)
        if subject_curie:
            qs = template.substitute(
                source=subject_key,
                target=object_key,
                source_id=subject_curie,
                target_id="",
            )
        else:
            qs = template.substitute(
                source=subject_key,
                target=object_key,
                target_id=object_curie,
                source_id="",
            )
        query = json.loads(qs)
        if subject_curie:
            del query["query_graph"]["nodes"][object_key]["ids"]
        else:
            del query["query_graph"]["nodes"][subject_key]["ids"]
        message = {
            "message": query,
            "parameters": query_body["parameters"],
        }
        if message["message"].get("knowledge_graph") is not None:
            del message["message"]["knowledge_graph"]
        message["parameters"] = query_body["parameters"]
        message["workflow"] = [{"id": "lookup"}]
        message["submitter"] = query_body["submitter"]
        filled_templates.append(message)

    return filled_templates


def expand_bte_query(query_dict: dict[str, Any], logger: logging.Logger) -> list[Any]:
    """Expand a given query into the appropriate templates."""
    # Contract:
    # 1. there is a single edge in the query graph
    # 2. The edge is marked inferred.
    # 3. Either the source or the target has IDs, but not both.
    # 4. The number of ids on the query node is 1.

    query_graph = query_dict["message"].get("query_graph")
    if query_graph is None:
        return []
    (
        subject_key,
        subject_type,
        subject_curie,
        object_key,
        object_type,
        object_curie,
        predicate,
        qualifiers,
    ) = get_params(query_graph)

    matched_template_paths = match_templates(
        subject_type,
        object_type,
        predicate,
        qualifiers,
        logger,
    )

    filled_templates = fill_templates(
        matched_template_paths,
        query_dict,
        subject_key,
        subject_curie,
        object_key,
        object_curie,
    )
    for qedge in query_graph["edges"].values():
        del qedge["knowledge_type"]

    expanded_queries = [
        {
            "message": {"query_graph": query_graph},
            "parameters": query_dict["parameters"],
            "submitter": query_dict["submitter"],
        }
    ]
    expanded_queries.extend(filled_templates)

    return expanded_queries


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, bte_lookup
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
            LOGGER.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
