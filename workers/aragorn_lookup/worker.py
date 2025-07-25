"""Aragorn ARA module."""

import asyncio
import copy
import httpx
import json
import logging
from pathlib import Path
from string import Template
import time
import uuid
from shepherd_utils.db import (
    get_message,
    get_running_callbacks,
    add_callback_id,
    save_message,
)
from shepherd_utils.config import settings
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "aragorn.lookup"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
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


async def aragorn_lookup(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(query_id, logger)
    try:
        infer, question_qnode, answer_qnode, pathfinder = examine_query(message)
    except Exception as e:
        logger.error(e)
        return None, 500

    if not infer:
        # Put callback UID and query ID in postgres
        callback_id = str(uuid.uuid4())[:8]
        await add_callback_id(query_id, callback_id, logger)
        # put lookup query graph in redis
        await save_message(
            f"{callback_id}_query_graph", message["message"]["query_graph"], logger
        )
        message["callback"] = f"{settings.callback_host}/callback/{callback_id}"

        async with httpx.AsyncClient(timeout=100) as client:
            await client.post(
                settings.kg_retrieval_url,
                json=message,
            )
    else:
        expanded_messages = expand_aragorn_query(message)
        requests = []
        # send all messages to retriever
        async with httpx.AsyncClient(timeout=100) as client:
            for expanded_message in expanded_messages:
                callback_id = str(uuid.uuid4())[:8]
                # Put callback UID and query ID in postgres
                await add_callback_id(query_id, callback_id, logger)
                # put lookup query graph in redis
                await save_message(
                    f"{callback_id}_query_graph",
                    expanded_message["message"]["query_graph"],
                    logger,
                )

                expanded_message["callback"] = (
                    f"{settings.callback_host}/callback/{callback_id}"
                )

                logger.debug(f"""Sending lookup query to {settings.kg_retrieval_url} with callback {expanded_message['callback']}""")
                request = client.post(
                    settings.kg_retrieval_url,
                    json=expanded_message,
                )
                requests.append(request)
                # Then we can retrieve all callback ids from query id to see which are still
                # being looked up
            # fire all the lookups at the same time
            await asyncio.gather(*requests)

    # this worker might have a timeout set for if the lookups don't finish within a certain
    # amount of time
    MAX_QUERY_TIME = 300
    start_time = time.time()
    running_callback_ids = ['']
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
        logger.warning(f"Timed out getting lookup callbacks. {len(running_callback_ids)} queries still running...")
    # TODO: clean up any on-going queries so they don't get merged in after we've already moved on and potentially overwrite with an old message

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


def get_infer_parameters(input_message):
    """Given an infer input message, return the parameters needed to run the infer.
    input_id: the curie of the input node
    predicate: the predicate of the inferred edge
    qualifiers: the qualifiers of the inferred edge
    source: the query node id of the source node
    target: the query node id of the target node
    source_input: True if the source node is the input node, False if the target node is the input node
    """
    for edge_id, edge in input_message["message"]["query_graph"]["edges"].items():
        source = edge["subject"]
        target = edge["object"]
        query_edge = edge_id
        predicate = edge["predicates"][0]
        qc = edge.get("qualifier_constraints", [])
        if len(qc) == 0:
            qualifiers = {}
        else:
            qualifiers = {"qualifier_constraints": qc}
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


def get_rule_key(predicate, qualifiers):
    keydict = {"predicate": predicate}
    keydict.update(qualifiers)
    return json.dumps(keydict, sort_keys=True)


def expand_aragorn_query(input_message):
    # Contract:
    # 1. there is a single edge in the query graph
    # 2. The edge is marked inferred.
    # 3. Either the source or the target has IDs, but not both.
    # 4. The number of ids on the query node is 1.
    input_id, predicate, qualifiers, source, source_input, target, qedge_id = (
        get_infer_parameters(input_message)
    )
    key = get_rule_key(predicate, qualifiers)
    # We want to run the non-inferred version of the query as well
    qg = copy.deepcopy(input_message["message"]["query_graph"])
    for eid, edge in qg["edges"].items():
        del edge["knowledge_type"]
    with open(
        Path(__file__).parent / "rules_with_types_cleaned_finalized.json", "r"
    ) as file:
        AMIE_EXPANSIONS = json.load(file)
    messages = [
        {
            "message": {"query_graph": qg},
            "parameters": input_message.get("parameters") or {},
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
        if "log_level" in input_message:
            message["log_level"] = input_message["log_level"]
        messages.append(message)
    return messages


async def process_task(task, parent_ctx, logger):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await aragorn_lookup(task, logger)
    finally:
        span.end()


async def poll_for_tasks():
    async for task, parent_ctx, logger in get_tasks(STREAM, GROUP, CONSUMER):
        asyncio.create_task(process_task(task, parent_ctx, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
