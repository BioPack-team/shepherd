"""Aragorn ARA module."""

import asyncio
import functools
import copy
import json
import logging
import uuid
from pathlib import Path
from string import Template


from shepherd_utils.config import settings
from shepherd_utils.db import get_message
from shepherd_utils.lookups import submit_lookups, wait_for_callbacks
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import (
    lookup_wait_seconds,
    run_task_lifecycle,
    run_poll_loop,
)

# Queue name
STREAM = "aragorn.lookup"
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


async def aragorn_lookup(task, logger: logging.Logger):
    """Do Aragorn lookup operation."""
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    response_id = task[1]["response_id"]
    message = await get_message(query_id, logger)
    parameters = message.get("parameters") or {}
    parameters["timeout"] = parameters.get("timeout", settings.lookup_timeout)
    parameters["tiers"] = parameters.get("tiers") or [settings.default_data_tier]
    use_gandalf = parameters.get("gandalf", False)
    message["parameters"] = parameters
    if "submitter" not in message:
        message["submitter"] = (
            "infores:shepherd-aragorn:{maturity}@{location}@{url}".format(
                maturity=settings.server_maturity,
                location=settings.server_location,
                url=settings.server_url,
            )
        )
    try:
        infer, question_qnode, answer_qnode, pathfinder = examine_query(message)
    except Exception as e:
        logger.error(e)
        return None, 500

    if not infer:
        # C6-3: the plain lookup goes through the same submission as the
        # expanded ones, so a rejected POST is an error rather than a silent
        # wait on a callback that never comes.
        lookups = [message]
    else:
        lookups = expand_aragorn_query(message, logger)
        logger.info(f"Expanded to {len(lookups)} messages")
    await submit_lookups(lookups, query_id, "aragorn", logger, span_name=STREAM)

    # this worker might have a timeout set for if the lookups don't finish within a certain
    # amount of time
    await wait_for_callbacks(
        query_id, response_id, lookup_wait_seconds(message["parameters"]), logger
    )


def get_infer_parameters(input_message):
    """Given an infer input message, return the parameters needed to run the infer.
    input_id: the curie of the input node
    predicate: the predicate of the inferred edge
    qualifiers: the inferred edge's TRAPI 2.0 ``constraints.qualifiers``, a list
        of ``{qualifier_type_id: qualifier_value}`` qualifier sets ([] if none)
    source: the query node id of the source node
    target: the query node id of the target node
    source_input: True if the source node is the input node, False if the target node is the input node
    """
    predicate = ""
    qualifiers: list[dict[str, str]] = []
    source = ""
    target = ""
    query_edge = ""
    for edge_id, edge in input_message["message"]["query_graph"]["edges"].items():
        source = edge["subject"]
        target = edge["object"]
        query_edge = edge_id
        predicate = edge["predicates"][0]
        qualifiers = list((edge.get("constraints") or {}).get("qualifiers") or [])
    if ("ids" in input_message["message"]["query_graph"]["nodes"][source]) and (
        input_message["message"]["query_graph"]["nodes"][source]["ids"] is not None
    ):
        input_id = input_message["message"]["query_graph"]["nodes"][source]["ids"][0]
        source_input = True
    else:
        input_id = input_message["message"]["query_graph"]["nodes"][target]["ids"][0]
        source_input = False
    # key = get_key(predicate, qualifiers)
    return input_id, predicate, qualifiers, source, source_input, target, query_edge


#: Qualifier types that select an AMIE rule set, mapped to their (internal,
#: un-prefixed) name in the keys of rules_with_types_cleaned_finalized.json.
RULE_KEY_QUALIFIERS = {
    "biolink:object_aspect_qualifier": "object_aspect_qualifier",
    "biolink:object_direction_qualifier": "object_direction_qualifier",
}


def get_rule_key(
    predicate: str,
    qualifiers: list[dict[str, str]],
    logger: logging.Logger,
) -> str:
    """Given some query parameters, construct a string key for expanded queries lookup.

    ``qualifiers`` is a QEdge's TRAPI 2.0 ``constraints.qualifiers``: a list of
    qualifier sets, each a ``{qualifier_type_id: qualifier_value}`` mapping. The
    key format is internal to the rules file and is unchanged from TRAPI 1.x.
    """
    keydict: dict[str, str] = {"predicate": predicate}
    if not qualifiers:
        return json.dumps(keydict)
    if len(qualifiers) > 1:
        logger.warning(
            "Got more than one qualifier constraint set, just using the first one."
        )
    qualifier_set = qualifiers[0] or {}
    if len(qualifier_set) < 1:
        return json.dumps(keydict)
    for qualifier_type_id, qualifier_value in qualifier_set.items():
        key_name = RULE_KEY_QUALIFIERS.get(qualifier_type_id)
        if key_name is not None:
            keydict[key_name] = qualifier_value
    return json.dumps(keydict, sort_keys=True)


@functools.cache
def _amie_expansions() -> dict:
    """The AMIE rules, read once per process: the file is ~358 KB and used to
    be re-read and re-parsed for every inferred query (#221, C6-8). Only read
    from, never written."""
    with open(
        Path(__file__).parent / "rules_with_types_cleaned_finalized.json", "r"
    ) as file:
        return json.load(file)


def expand_aragorn_query(input_message, logger: logging.Logger):
    """Given a query, split it into many related similar queries."""
    # Contract:
    # 1. there is a single edge in the query graph
    # 2. The edge is marked inferred.
    # 3. Either the source or the target has IDs, but not both.
    # 4. The number of ids on the query node is 1.
    input_id, predicate, qualifiers, source, source_input, target, qedge_id = (
        get_infer_parameters(input_message)
    )
    key = get_rule_key(predicate, qualifiers, logger)
    # We want to run the non-inferred version of the query as well
    qg = copy.deepcopy(input_message["message"]["query_graph"])
    for eid, edge in qg["edges"].items():
        del edge["knowledge_type"]
    AMIE_EXPANSIONS = _amie_expansions()
    messages = [
        {
            "message": {"query_graph": qg},
            "parameters": input_message["parameters"],
            "submitter": input_message["submitter"],
        }
    ]
    # If we don't have any AMIE expansions, this will just generate the direct query
    for rule_def in AMIE_EXPANSIONS.get(key, []):
        query_template = Template(json.dumps(rule_def["template"]))
        # need to do a bit of surgery depending on what the input is.
        if source_input:
            qs = query_template.substitute(
                source=source, target=target, source_id=input_id, target_id=""
            )
        else:
            qs = query_template.substitute(
                source=source, target=target, target_id=input_id, source_id=""
            )
        query = json.loads(qs)
        if source_input:
            del query["query_graph"]["nodes"][target]["ids"]
        else:
            del query["query_graph"]["nodes"][source]["ids"]
        message = {
            "message": query,
            "parameters": input_message.get("parameters") or {},
        }
        message["parameters"] = input_message["parameters"]
        message["submitter"] = input_message["submitter"]
        messages.append(message)
    return messages


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, aragorn_lookup
    )


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    await run_poll_loop(STREAM, GROUP, CONSUMER, TASK_LIMIT, process_task)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
