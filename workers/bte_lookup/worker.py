"""BTE ARA module."""

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Any, Dict, Optional

import httpx
from pydantic import BaseModel, parse_obj_as

from shepherd_utils.config import settings
from shepherd_utils.db import (
    add_callback_id,
    cleanup_callbacks,
    get_message,
    get_running_callbacks,
    remove_callback_id,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "bte.lookup"
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


@dataclass
class AsyncResponse:
    status_code: int
    success: bool
    callback_id: str
    error: Optional[str] = None


async def run_async_lookup(
    client: httpx.AsyncClient,
    message: dict,
    callback_id: str,
) -> AsyncResponse:
    """Return an async lookup response with callback id."""
    try:
        response = await client.post(
            settings.kg_retrieval_url,
            json=message,
        )
        return AsyncResponse(
            status_code=response.status_code,
            success=response.status_code == 200,
            callback_id=callback_id,
        )
    except Exception as e:
        return AsyncResponse(
            status_code=500,
            success=False,
            callback_id=callback_id,
            error=str(e),
        )


async def bte_lookup(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(query_id, logger)
    parameters = message.get("parameters") or {}
    parameters["timeout"] = parameters.get("timeout", settings.lookup_timeout)
    parameters["tiers"] = parameters.get("tiers") or [1]
    message["parameters"] = parameters
    try:
        infer, question_qnode, answer_qnode, pathfinder = examine_query(message)
    except Exception as e:
        logger.error(e)
        return None, 500
    if pathfinder:
        # BTE currently doesn't handle Pathfinder queries
        return None, 500

    if not infer:
        # Put callback UID and query ID in postgres
        callback_id = str(uuid.uuid4())[:8]
        await add_callback_id(query_id, callback_id, logger)
        message["callback"] = f"{settings.callback_host}/bte/callback/{callback_id}"

        async with httpx.AsyncClient(timeout=100) as client:
            await client.post(
                settings.kg_retrieval_url,
                json=message,
            )
    else:
        expanded_messages = expand_bte_query(message, logger)
        logger.info(f"Expanded to {len(expanded_messages)} messages")
        requests = []
        # send all messages to retriever
        async with httpx.AsyncClient(timeout=20) as client:
            for expanded_message in expanded_messages:
                callback_id = str(uuid.uuid4())[:8]
                # Put callback UID and query ID in postgres
                await add_callback_id(query_id, callback_id, logger)

                expanded_message["callback"] = (
                    f"{settings.callback_host}/bte/callback/{callback_id}"
                )

                logger.debug(
                    f"""Sending lookup query to {settings.kg_retrieval_url} with callback {expanded_message['callback']}"""
                )
                requests.append(run_async_lookup(client, expanded_message, callback_id))
                # Then we can retrieve all callback ids from query id to see which are still
                # being looked up
            # fire all the lookups at the same time
            responses = await asyncio.gather(*requests, return_exceptions=True)

            for response in responses:
                if isinstance(response, Exception):
                    logger.error(
                        f"Failed to do lookup and unable to remove callback id: {response}"
                    )
                elif isinstance(response, AsyncResponse):
                    if not response.success:
                        logger.error(
                            f"Failed to do lookup, removing callback id: {response.error}"
                        )
                        await remove_callback_id(response.callback_id, logger)
                else:
                    logger.error(
                        f"Failed to do lookup and unable to remove callback id: {response}"
                    )

    # this worker might have a timeout set for if the lookups don't finish within a certain
    # amount of time
    MAX_QUERY_TIME = message["parameters"]["timeout"]
    start_time = time.time()
    running_callback_ids = [""]
    while time.time() - start_time < MAX_QUERY_TIME:
        # see if there are existing lookups going
        running_callback_ids = await get_running_callbacks(query_id, logger)
        # logger.info(f"Got back {len(running_callback_ids)} running lookups")
        # if there are, continue to wait
        if len(running_callback_ids) > 0:
            await asyncio.sleep(1)
            continue
        # if there aren't, lookup is complete and we need to pass on to next workflow operation
        if len(running_callback_ids) == 0:
            logger.debug("Got all lookups back. Continuing...")
            break

    if time.time() - start_time > MAX_QUERY_TIME:
        logger.warning(
            f"Timed out getting lookup callbacks. {len(running_callback_ids)} queries were still running..."
        )
        await cleanup_callbacks(query_id, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


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

    qualifier_constraints = edge.get("qualifier_constraints") or []
    if qualifier_constraints is not None and len(qualifier_constraints) > 0:
        qualifiers = {
            qualifier["qualifier_type_id"]: qualifier["qualifier_value"]
            for qualifier in qualifier_constraints[0]["qualifier_set"]
        }

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

    with open(
        Path(__file__).parent / "template_groups.json", "r", encoding="utf-8"
    ) as file:
        templateGroups = parse_obj_as(list[TemplateGroup], json.load(file))

    template_paths = {
        path.name: path
        for path in (Path(__file__).parent / "templates").rglob("*.json")
    }

    matched_paths: set[Path] = set()
    for group in templateGroups:
        conditions: list[bool] = []
        conditions.append(len(subject_types.intersection(group.subject)) > 0)
        conditions.append(len(object_types.intersection(group.object)) > 0)
        conditions.append(len(predicates.intersection(group.predicate)) > 0)
        conditions.append(  # Qualifiers (if they exist) are satisfied
            all(
                (group.qualifiers or {}).get(qualifier_type, False) == value
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
        with open(path, "r", encoding="utf-8") as file:
            template = Template(file.read())
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
        if "log_level" in query_body:
            message["log_level"] = query_body["log_level"]
        if message["message"].get("knowledge_graph") is not None:
            del message["message"]["knowledge_graph"]
        message["parameters"] = query_body["parameters"]
        message["workflow"] = [{"id": "lookup"}]
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
        }
    ]
    expanded_queries.extend(filled_templates)

    return expanded_queries


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await bte_lookup(task, logger)
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
