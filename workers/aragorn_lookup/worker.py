"""Aragorn ARA module."""

import asyncio
import copy
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import Optional

import httpx
from opentelemetry.propagate import inject

from shepherd_utils.config import Settings, settings
from shepherd_utils.db import (
    add_callback_id,
    cleanup_callbacks,
    get_message,
    get_running_callbacks,
    remove_callback_id,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, run_task_lifecycle

from . import query_templates
from .probe import probe_disease

# Queue name
STREAM = "aragorn.lookup"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)
# Startup logging has to go through a "shepherd.*" logger: shepherd_utils.shared
# calls setup_logging() on import, which attaches handlers to the "shepherd"
# logger and leaves the root logger bare. Anything logged on the root is
# dropped by logging.lastResort below WARNING, which is why a worker appears
# silent until its first task arrives and get_tasks builds a named logger.
startup_logger = logging.getLogger(f"shepherd.{STREAM}")

# Creative-edge predicates the census template portfolio answers. The portfolio
# is drug-for-disease, so anything else -- contraindication, the qualified
# affects rules -- keeps using the AMIE expansions regardless of template_set.
TREATS_PREDICATES = frozenset(
    {
        "biolink:treats",
        "biolink:treats_or_applied_or_studied_to_treat",
    }
)

TEMPLATE_SETS = frozenset({"census", "amie", "both"})

# Creative edges the census portfolio answers, and the graph predicates behind
# them. Aragorn's AMIE rule keys use biolink:contraindicated_for, but this graph
# records biolink:contraindicated_in (22,859 edges) -- both are accepted.
DIRECTION_QUALIFIER = "biolink:object_direction_qualifier"
AFFECTS_PREDICATES = frozenset({"biolink:affects"})
CONTRAINDICATED_PREDICATES = frozenset(
    {"biolink:contraindicated_for", "biolink:contraindicated_in"}
)


def creative_query_type(predicate, qualifiers, source_input):
    """Which census portfolio answers this creative edge, and in which direction.

    Returns ``(query_type, direction)``, or ``(None, None)`` when nothing in the
    portfolio covers the edge and the AMIE rules should handle it.

    ``source_input`` says whether the *subject* of the qedge is the pinned node.
    For every one of these predicates the subject is the chemical, so a pinned
    subject means "what does this chemical do" and a pinned object means "what
    acts on this thing".
    """
    if predicate in TREATS_PREDICATES and not qualifiers:
        # Only the disease-pinned form; "what does this drug treat" has no
        # templates.
        return ("treats", None) if not source_input else (None, None)
    if predicate in CONTRAINDICATED_PREDICATES and not qualifiers:
        return ("contraindicated", None) if not source_input else (None, None)
    if predicate in AFFECTS_PREDICATES:
        direction = _requested_direction(qualifiers)
        if direction is None:
            # An affects query with no direction asked for has no sign to
            # propagate, so the sign-carrying templates cannot be built.
            return None, None
        query_type = (
            "affects_chemical_pinned" if source_input else "affects_gene_pinned"
        )
        return query_type, direction
    return None, None


def _requested_direction(qualifiers):
    """The object_direction_qualifier the query asked for, if any."""
    for constraint in (qualifiers or {}).get("qualifier_constraints", []) or []:
        for qualifier in constraint.get("qualifier_set", []) or []:
            if qualifier.get("qualifier_type_id") == DIRECTION_QUALIFIER:
                value = qualifier.get("qualifier_value")
                if value in ("increased", "decreased"):
                    return value
    return None


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


@dataclass
class AsyncResponse:
    status_code: int
    success: bool
    callback_id: str
    error: Optional[str] = None


async def run_async_lookup(
    client: httpx.AsyncClient,
    message: dict,
    query_id: str,
    logger: logging.Logger,
    label: str = "",
) -> AsyncResponse:
    """Return an async lookup response with callback id.

    ``label`` names the expansion that produced this message (a census template
    name, or ``direct_lookup``). It is recorded on the span and in the log so an
    A/B run can attribute a callback back to the template that asked for it.
    """
    callback_id = str(uuid.uuid4())[:8]
    with tracer.start_as_current_span("aragorn.lookup") as span:
        span.set_attribute("callback_id", callback_id)
        if label:
            span.set_attribute("template", label)
        lookup_carrier = {}
        inject(lookup_carrier)
        # Put callback UID and query ID in postgres
        await add_callback_id(query_id, callback_id, json.dumps(lookup_carrier), logger)

        message["callback"] = f"{settings.callback_host}/aragorn/callback/{callback_id}"

        logger.debug(
            f"""Sending lookup query ({label or "unlabelled"}) to {settings.kg_retrieval_url} with callback {message['callback']}"""
        )
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
            span.record_exception(e)
            return AsyncResponse(
                status_code=500,
                success=False,
                callback_id=callback_id,
                error=str(e),
            )


async def aragorn_lookup(task, logger: logging.Logger):
    """Do Aragorn lookup operation."""
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    response_id = task[1]["response_id"]
    otel = task[1]["otel"]
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
        # Put callback UID and query ID in postgres
        callback_id = str(uuid.uuid4())[:8]
        await add_callback_id(query_id, callback_id, otel, logger)
        message["callback"] = f"{settings.callback_host}/aragorn/callback/{callback_id}"
        # with open("./debug/direct_query.json", "w", encoding="utf-8") as f:
        #     json.dump(message, f, indent=2)

        logger.debug(f"""Sending lookup query to {settings.kg_retrieval_url}.""")
        with tracer.start_as_current_span("aragorn.lookup") as span:
            span.set_attribute("callback_id", callback_id)
            async with httpx.AsyncClient(timeout=100) as client:
                await client.post(
                    settings.kg_retrieval_url,
                    json=message,
                )
    else:
        expanded_messages, labels = await build_lookup_messages(message, logger)
        # with open("./debug/expanded_messages.json", "w", encoding="utf-8") as f:
        #     json.dump(expanded_messages, f, indent=2)

        requests = []
        # send all messages to lookup service
        async with httpx.AsyncClient(timeout=20) as client:
            for expanded_message, label in zip(expanded_messages, labels):
                requests.append(
                    run_async_lookup(client, expanded_message, query_id, logger, label)
                )
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
        try:
            # see if there are existing lookups going
            running_callback_ids = await get_running_callbacks(query_id, logger)
        except Exception:
            # Brief backoff then retry the check rather than giving up
            await asyncio.sleep(5)
            continue
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
            f"Timed out getting lookup callbacks. {len(running_callback_ids)} queries were still running...{running_callback_ids}"
        )
        # logger.warning(f"Running callbacks: {running_callback_ids}")
        await cleanup_callbacks(query_id, logger)


def get_infer_parameters(input_message):
    """Given an infer input message, return the parameters needed to run the infer.
    input_id: the curie of the input node
    predicate: the predicate of the inferred edge
    qualifiers: the qualifiers of the inferred edge
    source: the query node id of the source node
    target: the query node id of the target node
    source_input: True if the source node is the input node, False if the target node is the input node
    """
    predicate = ""
    qualifiers = {}
    source = ""
    target = ""
    query_edge = ""
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


def get_rule_key(
    predicate: str,
    qualifiers: dict[str, list],
    logger: logging.Logger,
) -> str:
    """Given some query parameters, construct a string key for expanded queries lookup."""
    keydict: dict[str, str] = {"predicate": predicate}
    if len(qualifiers.keys()) > 0:
        # this is a bunch of logic to parse the dict of list of dicts of lists
        # We're currently expecting it to be a specific format with specific keys
        qualifier_constraints = qualifiers.get("qualifier_constraints", [])
        if len(qualifier_constraints) < 1:
            return json.dumps(keydict)
        if len(qualifier_constraints) > 1:
            logger.warning(
                "Got more than one qualifier_constraints dict, just using the first one."
            )
        qualifier_set = qualifier_constraints[0].get("qualifier_set", [])
        if len(qualifier_set) < 1:
            return json.dumps(keydict)
        for qualifier in qualifier_set:
            if qualifier.get("qualifier_type_id") == "biolink:object_aspect_qualifier":
                keydict["object_aspect_qualifier"] = qualifier.get("qualifier_value")
            elif (
                qualifier.get("qualifier_type_id")
                == "biolink:object_direction_qualifier"
            ):
                keydict["object_direction_qualifier"] = qualifier.get("qualifier_value")
    return json.dumps(keydict, sort_keys=True)


def describe_expansion_config() -> str:
    """The effective creative-expansion config, and what set each value.

    ``Settings`` resolves environment variables and ``.env`` ahead of the
    defaults written in ``shepherd_utils/config.py``, and compose mounts the
    repo's ``.env`` into the worker. So editing a default and deploying it can
    silently change nothing, with the only evidence being which templates show
    up in the lookup logs. This says so directly at startup.
    """
    fields = (
        "template_set",
        "template_tiers",
        "template_exclude_leaky",
        "template_path_budget",
        "template_probe_enabled",
        "census_dir",
    )
    parts = []
    for name in fields:
        value = getattr(settings, name)
        default = Settings.model_fields[name].default
        if value == default:
            parts.append(f"{name}={value!r}")
        else:
            # Environment wins over .env, so check it first.
            source = "env" if name.upper() in os.environ else ".env"
            parts.append(f"{name}={value!r} [{source}, overriding {default!r}]")
    return "Aragorn creative expansion: " + ", ".join(parts)


@lru_cache(maxsize=1)
def get_amie_expansions() -> dict:
    """The mined AMIE rules, parsed once per process.

    This is ~400KB of JSON that used to be re-read and re-parsed on every
    creative query.
    """
    with open(
        Path(__file__).parent / "rules_with_types_cleaned_finalized.json", "r"
    ) as file:
        return json.load(file)


@lru_cache(maxsize=1)
def get_census():
    """The metagraph census, loaded once per process (None if not mounted)."""
    return query_templates.load_census(settings.census_dir)


def build_direct_message(input_message) -> dict:
    """The plain (non-inferred) form of the original query.

    Every expansion set is fired alongside this one, and ``merge_message``
    recognises it by shape (``queries_equivalent``) to treat its results as
    lookup rather than creative results. It must therefore appear exactly once,
    which is why it is built here rather than inside either expander.
    """
    qg = copy.deepcopy(input_message["message"]["query_graph"])
    for _, edge in qg["edges"].items():
        edge.pop("knowledge_type", None)
    return {
        "message": {"query_graph": qg},
        "parameters": copy.deepcopy(input_message.get("parameters") or {}),
        "submitter": input_message["submitter"],
    }


def census_templates_applicable(predicate, qualifiers, source_input) -> bool:
    """Whether any census portfolio can answer this creative edge."""
    return creative_query_type(predicate, qualifiers, source_input)[0] is not None


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
    AMIE_EXPANSIONS = get_amie_expansions()
    messages = [build_direct_message(input_message)]
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
            # Copied, not shared: each expansion carries its own parameters so
            # a per-query edit (the census path sets filter_config per
            # template) cannot leak across the other expansions.
            "parameters": copy.deepcopy(input_message.get("parameters") or {}),
            "submitter": input_message["submitter"],
        }
        if "log_level" in input_message:
            message["log_level"] = input_message["log_level"]
        messages.append(message)
    return messages


async def expand_census_query(input_message, logger: logging.Logger):
    """Expand a drug-for-disease creative query into the census portfolio.

    Returns ``(messages, labels)``: the TRAPI requests to fire and the template
    name behind each, in the same order. The labels are logged against the
    callback ids so an A/B run can attribute results back to a template.

    The direct (non-inferred) lookup query is *not* included -- the caller adds
    it once, so it is not duplicated when both template sets fire.
    """
    input_id, predicate, qualifiers, source, source_input, target, qedge_id = (
        get_infer_parameters(input_message)
    )
    query_type, direction = creative_query_type(predicate, qualifiers, source_input)
    if query_type is None:
        return [], []
    # Whichever end carries the ids is the question; the other is the answer.
    question_qnode, answer_qnode = (
        (source, target) if source_input else (target, source)
    )
    qnodes = input_message["message"]["query_graph"]["nodes"]
    answer_categories = qnodes.get(answer_qnode, {}).get("categories") or []

    parameters = input_message.get("parameters") or {}
    requested = parameters.get("templates")
    exclude_leaky = parameters.get("exclude_leaky", settings.template_exclude_leaky)
    tiers = parameters.get("template_tiers") or settings.template_tier_list
    candidates = [
        template
        for template in query_templates.templates_for(query_type)
        if template.answer_compatible(answer_categories)
        and (requested is None or template.name in requested)
        and (not tiers or template.tier in tiers)
        and not (exclude_leaky and template.leaky)
    ]
    if not candidates:
        logger.warning(
            "No %s templates match this query (answer categories %s); "
            "falling back to the AMIE expansions.",
            query_type,
            answer_categories,
        )
        return [], []

    probe = {}
    if parameters.get("probe", settings.template_probe_enabled):
        with tracer.start_as_current_span("aragorn.lookup.probe"):
            probe = await probe_disease(
                input_id, query_templates.probe_specs_for(candidates), logger
            )

    budget = parameters.get("template_path_budget", settings.template_path_budget)
    skipped: list = []
    selected = query_templates.select_portfolio(
        candidates,
        get_census(),
        probe=probe or None,
        budget=budget,
        tiers=tiers,
        answer_categories=answer_categories,
        skipped=skipped,
        direction=direction,
    )
    if not selected:
        logger.warning(
            "Census portfolio selected nothing for %s; "
            "falling back to the AMIE expansions.",
            input_id,
        )
        return [], []

    messages, labels = [], []
    for template, summary in selected:
        query_graph = template.render(
            input_id,
            question_qnode,
            answer_qnode,
            pinned_node=qnodes.get(question_qnode),
            answer_node=qnodes.get(answer_qnode),
            direction=direction,
        )
        template_parameters = copy.deepcopy(parameters)
        # The template's degree caps are defaults; anything the caller set
        # explicitly wins, so a query can still tighten or loosen them.
        filter_config = dict(template.filter_config)
        filter_config.update(template_parameters.get("filter_config") or {})
        if filter_config:
            template_parameters["filter_config"] = filter_config
        message = {
            "message": {"query_graph": query_graph},
            "parameters": template_parameters,
            "submitter": input_message["submitter"],
        }
        if "log_level" in input_message:
            message["log_level"] = input_message["log_level"]
        messages.append(message)
        labels.append(template.name)

    # Say where the estimates came from on every query, not only in the startup
    # line. The baselines were derived from the census, so an unmounted census
    # produces the same numbers for an average disease and is otherwise
    # invisible -- it only diverges in the tail, which is where it matters.
    priced_from = (
        "baselines (no census mounted)"
        if any(summary.get("source") == "baseline" for _, summary in selected)
        else "census"
    )
    logger.info(
        "Census portfolio (%s%s) for %s: %d templates, ~%d expected paths, "
        "priced from %s%s (%s)",
        query_type,
        f", {direction}" if direction else "",
        input_id,
        len(selected),
        sum(summary["expected_paths"] for _, summary in selected),
        priced_from,
        ", probed" if probe else ", unprobed",
        ", ".join(
            f"{template.name}:{summary['expected_paths']}"
            for template, summary in selected
        ),
    )
    for template, summary in skipped:
        logger.warning(
            "Template %s configured but NOT fired: %s.",
            template.name,
            summary["skipped"],
        )
    return messages, labels


async def build_lookup_messages(input_message, logger: logging.Logger):
    """Build every lookup request for a creative query, with a label each.

    ``template_set`` picks the expansion strategy -- ``parameters.template_set``
    per query, falling back to ``settings.template_set``. Census templates only
    cover drug-for-disease, so any other creative edge uses AMIE whatever the
    setting says; that keeps the A/B honest instead of silently answering some
    queries with nothing.
    """
    parameters = input_message.get("parameters") or {}
    template_set = parameters.get("template_set") or settings.template_set
    if template_set not in TEMPLATE_SETS:
        logger.warning(
            "Unknown template_set %r; using %r.", template_set, settings.template_set
        )
        template_set = settings.template_set

    _, predicate, qualifiers, _, source_input, _, _ = get_infer_parameters(
        input_message
    )
    applicable = census_templates_applicable(predicate, qualifiers, source_input)
    if template_set in ("census", "both") and not applicable:
        logger.debug(
            "No census templates for a %s creative edge; using AMIE expansions.",
            predicate,
        )
        template_set = "amie"

    messages = [build_direct_message(input_message)]
    labels = ["direct_lookup"]

    if template_set in ("census", "both"):
        census_messages, census_labels = await expand_census_query(
            input_message, logger
        )
        messages.extend(census_messages)
        labels.extend(census_labels)
        if not census_messages:
            # expand_census_query said why; fall back rather than fire nothing.
            template_set = "amie" if template_set == "census" else template_set

    if template_set in ("amie", "both"):
        # expand_aragorn_query builds its own direct query; drop that copy so
        # merge_message sees exactly one.
        messages.extend(expand_aragorn_query(input_message, logger)[1:])
        labels.extend(f"amie_{index}" for index in range(len(messages) - len(labels)))

    return messages, labels


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, aragorn_lookup
    )


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    startup_logger.info(describe_expansion_config())
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
