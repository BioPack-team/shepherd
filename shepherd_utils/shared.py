"""Shared Shepherd Utility Functions."""

import asyncio
import json
import logging
import time
from typing import AsyncGenerator, Dict, List, Tuple

from opentelemetry.context.context import Context
from opentelemetry.propagate import extract

from .broker import add_task, broker_client, get_task, mark_task_as_complete
from .config import settings
from .db import initialize_db, save_logs
from .heartbeat import Heartbeat
from .logger import QueryLogger, setup_logging
from .reclaim import reclaim_orphaned

# Cap each per-stream duration queue so a stopped monitor can't OOM the broker.
# 10k entries per stream is well above what we'd accumulate in a 30s drain
# window even at peak load.
_DURATION_QUEUE_CAP = 10000


def _duration_key(stream: str) -> str:
    return f"monitor:task_durations:{stream}"


async def _record_task_duration(
    stream: str,
    started_at_str: str,
    logger: logging.Logger,
) -> None:
    """Push ``ms_elapsed`` onto the per-stream duration list for the monitor."""
    if not started_at_str:
        return
    try:
        duration_ms = max(0, int((time.time() - float(started_at_str)) * 1000))
    except (TypeError, ValueError):
        return
    try:
        pipe = broker_client.pipeline()
        pipe.lpush(_duration_key(stream), str(duration_ms))
        pipe.ltrim(_duration_key(stream), 0, _DURATION_QUEUE_CAP - 1)
        await pipe.execute()
    except Exception as e:
        logger.debug(f"Failed to record task duration for {stream}: {e}")


setup_logging()


def get_next_operation(
    workflow: List[Dict[str, str]],
) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """
    Get the next workflow operation from the list.

    Args:
        workflow (List[Dict[str, str]]): TRAPI workflow operation list
    """
    next_op = workflow[0]
    return next_op, workflow


def _build_task_context(
    stream: str,
    consumer: str,
    ara_task,
    level_number: int,
) -> Tuple[Context, logging.Logger]:
    """Build the per-task logger and otel context for a fetched/reclaimed task."""
    log_handler = QueryLogger().log_handler
    task_logger = logging.getLogger(
        f"shepherd.{stream}.{consumer}.{ara_task[1]['query_id']}"
    )
    task_log_level = int(ara_task[1].get("log_level", level_number))
    task_logger.setLevel(task_log_level)
    task_logger.addHandler(log_handler)
    task_logger.info(f"Doing task {ara_task}")
    ctx = extract(json.loads(ara_task[1].get("otel", "{}")))
    # Stamp the task payload with our delivery time so wrap_up_task /
    # handle_task_failure can compute the per-task latency without touching
    # every individual worker. Only set if not already present so a reclaimed
    # task keeps its original start time.
    if "_started_at" not in ara_task[1]:
        ara_task[1]["_started_at"] = str(time.time())
    return ctx, task_logger


async def get_tasks(
    stream: str,
    group: str,
    consumer: str,
    task_limit: int,
    reclaim_min_idle_sec: int = None,
) -> AsyncGenerator[
    Tuple[Tuple[str, str], Context, logging.Logger, asyncio.Semaphore], None
]:
    """Continually monitor the ara queue for tasks.

    ``reclaim_min_idle_sec`` overrides the per-stream default for how long a
    message must be idle before another consumer can XCLAIM it. Pass an
    explicit value when the worker knows its worst-case task duration; leave
    it ``None`` to fall back to ``PER_STREAM_MIN_IDLE_SEC`` / settings.
    """
    # Set up logger
    level_number = logging._nameToLevel[settings.log_level]
    log_handler = QueryLogger().log_handler
    worker_logger = logging.getLogger(f"shepherd.{stream}.{consumer}")
    worker_logger.setLevel(level_number)
    worker_logger.addHandler(log_handler)
    # initialize opens the db connection
    await initialize_db()
    task_limiter = asyncio.Semaphore(task_limit)
    # register this worker with the monitor via a Redis heartbeat key
    Heartbeat(stream, consumer, task_limit).start()
    # periodic orphan-task reclaim so a worker crash doesn't strand its PEL
    reclaim_interval = max(5.0, float(settings.reclaim_interval_sec))
    last_reclaim = 0.0
    # continuously poll the broker for new tasks
    while True:
        # Before fetching new work, check whether any pending messages on this
        # stream belong to a dead consumer and claim them. Heartbeat + idle
        # filtering inside ``reclaim_orphaned`` keep live consumers safe.
        now = time.time()
        if now - last_reclaim >= reclaim_interval:
            last_reclaim = now
            try:
                reclaimed = await reclaim_orphaned(
                    stream,
                    group,
                    consumer,
                    worker_logger,
                    min_idle_sec=reclaim_min_idle_sec,
                )
            except Exception as e:
                worker_logger.error(f"Reclaim sweep failed for {stream}: {e}")
                reclaimed = []
            for ara_task in reclaimed:
                await task_limiter.acquire()
                try:
                    ctx, task_logger = _build_task_context(
                        stream, consumer, ara_task, level_number
                    )
                except Exception as e:
                    worker_logger.error(
                        f"Failed to build context for reclaimed task {ara_task}: {e}"
                    )
                    task_limiter.release()
                    continue
                yield ara_task, ctx, task_logger, task_limiter

        # check if we can take another task
        await task_limiter.acquire()
        # get a new task for the given target
        ara_task = await get_task(stream, group, consumer, worker_logger)
        if ara_task is not None:
            ctx, task_logger = _build_task_context(
                stream, consumer, ara_task, level_number
            )
            # send the task to a async background task
            # this could be async, multi-threaded, etc.
            yield ara_task, ctx, task_logger, task_limiter
        else:
            task_limiter.release()


async def wrap_up_task(
    stream: str,
    group: str,
    task: tuple[str, dict],
    logger: logging.Logger,
):
    """Call the next task and mark this one as complete."""
    workflow = json.loads(task[1]["workflow"])
    # remove the operation we just did
    if stream == workflow[0]["id"]:
        # make sure the worker is in the workflow
        # for entry workers, they won't match and we'll do the first operation
        workflow.pop(0)
    # grab the next operation in the list
    if len(workflow) > 0:
        next_op = workflow[0]["id"]
    else:
        next_op = "finish_query"
    logger.info(f"Sending task to {next_op}")
    await add_task(
        next_op,
        {
            "query_id": task[1]["query_id"],
            "response_id": task[1]["response_id"],
            "workflow": json.dumps(workflow),
            "log_level": task[1].get("log_level", 20),
            "otel": task[1]["otel"],
            "metadata": task[1]["metadata"],
        },
        logger,
    )

    await mark_task_as_complete(stream, group, task[0], logger)
    await save_logs(task[1]["response_id"], logger)
    await _record_task_duration(stream, task[1].get("_started_at", ""), logger)


async def handle_task_failure(
    stream: str,
    group: str,
    task: Tuple[str, dict],
    logger: logging.Logger,
) -> None:
    """Handle any full query failures."""
    await mark_task_as_complete(stream, group, task[0], logger)
    await save_logs(task[1]["response_id"], logger)
    await _record_task_duration(stream, task[1].get("_started_at", ""), logger)
    logger.error("Sending task straight to finish_query.")
    await add_task(
        "finish_query",
        {
            "query_id": task[1]["query_id"],
            "response_id": task[1]["response_id"],
            "workflow": "[]",
            "log_level": task[1].get("log_level", 20),
            "otel": task[1]["otel"],
            "status": "ERROR",
            "metadata": task[1]["metadata"],
        },
        logger,
    )


def recursive_get_edge_support_graphs(
    edge: str,
    edges: set,
    auxgraphs: set,
    message_edges: dict,
    message_auxgraphs: dict,
    nodes: set,
):
    """Recursive method to find auxiliary graphs to keep when filtering. Each auxiliary
    graph then has its edges filterd."""
    if edge in edges:
        # Already visited; short-circuit to avoid exponential re-traversal
        # when many edges/aux graphs share the same support structure.
        return edges, auxgraphs, nodes
    edges.add(edge)
    edge_data = message_edges[edge]
    nodes.add(edge_data["subject"])
    nodes.add(edge_data["object"])
    for attribute in edge_data.get("attributes", []) or []:
        if attribute.get("attribute_type_id") == "biolink:support_graphs":
            for auxgraph in attribute.get("value", []):
                if auxgraph not in message_auxgraphs:
                    raise KeyError(f"auxgraph {auxgraph} not in auxiliary_graphs")
                edges, auxgraphs, nodes = recursive_get_auxgraph_edges(
                    auxgraph,
                    edges,
                    auxgraphs,
                    message_edges,
                    message_auxgraphs,
                    nodes,
                )
    return edges, auxgraphs, nodes


def recursive_get_auxgraph_edges(
    auxgraph: str,
    edges: set,
    auxgraphs: set,
    message_edges: dict,
    message_auxgraphs: dict,
    nodes: set,
):
    """Recursive method to find edges to keep when filtering. Each edge then
    has support graphs filtered."""
    if auxgraph in auxgraphs:
        return edges, auxgraphs, nodes
    auxgraphs.add(auxgraph)
    aux_edges = message_auxgraphs.get(auxgraph, {}).get("edges", [])
    for aux_edge in aux_edges:
        if aux_edge not in message_edges:
            raise KeyError(f"aux_edge {aux_edge} not in knowledge_graph.edges")
        edges, auxgraphs, nodes = recursive_get_edge_support_graphs(
            aux_edge, edges, auxgraphs, message_edges, message_auxgraphs, nodes
        )
    return edges, auxgraphs, nodes


def is_support_edge(edge) -> bool:
    """Checks if a given edge is a support edge."""
    if "attributes" not in edge:
        return False
    for attribute in edge["attributes"]:
        if attribute["attribute_type_id"] == "biolink:support_graphs":
            return True
    return False


def validate_message(message, logger):
    """Validate a given message for missing nodes."""
    valid = True
    for edge_id, edge in message["message"]["knowledge_graph"]["edges"].items():
        try:
            # print(f"Checking {edge_id}")
            assert edge["subject"] in message["message"]["knowledge_graph"]["nodes"]
            assert edge["object"] in message["message"]["knowledge_graph"]["nodes"]
            for attribute in edge.get("attibutes", []):
                if attribute["attribute_type_id"] == "biolink:support_graphs":
                    for value in attribute["value"]:
                        if value not in message["message"].get("auxiliary_graphs", {}):
                            raise AssertionError(
                                f"Aux graph {value} is not in the aux graphs."
                            )
        except AssertionError as e:
            valid = False
            logger.error(f"Edge {edge_id} has issues: {e}")
    if not valid:
        with open("invalid_message.json", "w", encoding="utf-8") as f:
            json.dump(message, f, indent=2)


def combine_unique_dicts(list1, list2, logger: logging.Logger):
    """Combine two lists of dicts, keeping only unique dictionaries.

    Uses ``json.dumps(..., sort_keys=True)`` as a stable signature -- it's
    implemented in C and faster than the recursive Python hashing the prior
    implementation used. ``default=str`` keeps it forgiving for the rare
    non-JSON-serializable value (datetime, Decimal, etc.) instead of dropping
    the item silently.
    """
    seen = set()
    result = []
    for d in list1:
        try:
            sig = json.dumps(d, sort_keys=True, default=str)
        except (TypeError, ValueError):
            logger.error(f"Failed to hash this: {d}")
            continue
        if sig not in seen:
            seen.add(sig)
            result.append(d)
    for d in list2:
        try:
            sig = json.dumps(d, sort_keys=True, default=str)
        except (TypeError, ValueError):
            logger.error(f"Failed to hash this: {d}")
            continue
        if sig not in seen:
            seen.add(sig)
            result.append(d)
    return result


def merge_kgraph(og_message, new_message, source, logger: logging.Logger):
    """Merge ``new_message`` into ``og_message`` in place and return it.

    Previously this allocated a deep copy of ``og_message`` and mutated that.
    The deep copy dominated runtime on large kgraphs (thousands of edges,
    each with attribute lists). The accumulator-style call sites
    (``acc = merge_kgraph(acc, kg, ...)``) discard ``og_message`` after
    each call, so mutating it directly is safe and dramatically faster.
    Newly adopted nodes/edges are not copied either -- ``new_message``
    is also discarded by the caller after merging.
    """
    aggregator_source = {
        "resource_id": source,
        "resource_role": "aggregator_knowledge_source",
        "upstream_resource_ids": ["infores:retriever"],
    }
    og_nodes = og_message["nodes"]
    og_edges = og_message["edges"]

    for key, value in new_message["nodes"].items():
        existing = og_nodes.get(key)
        if existing is None:
            og_nodes[key] = value
            continue
        # Overlapping node: merge fields onto the existing entry.
        if value["name"]:
            existing["name"] = value["name"]
        new_categories = value["categories"]
        if new_categories:
            existing_categories = existing["categories"]
            if existing_categories:
                existing["categories"] = list(
                    set(existing_categories) | set(new_categories)
                )
            else:
                existing["categories"] = new_categories
        new_attrs = value["attributes"]
        if new_attrs:
            existing_attrs = existing["attributes"]
            if existing_attrs:
                existing["attributes"] = combine_unique_dicts(
                    existing_attrs, new_attrs, logger
                )
            else:
                existing["attributes"] = new_attrs

    for key, value in new_message["edges"].items():
        existing = og_edges.get(key)
        if existing is None:
            og_edges[key] = value
            sources = value.get("sources")
            if sources and not is_support_edge(value):
                # Append the aggregator source if it isn't already present.
                # Avoids the heavy combine_unique_dicts hashing for what is
                # almost always a 3-element list.
                if aggregator_source not in sources:
                    sources.append(aggregator_source)
            continue
        # Overlapping edge: merge attributes and sources.
        new_attrs = value["attributes"]
        if new_attrs:
            existing_attrs = existing["attributes"]
            if existing_attrs:
                existing["attributes"] = combine_unique_dicts(
                    existing_attrs, new_attrs, logger
                )
            else:
                existing["attributes"] = new_attrs

        new_sources = value["sources"]
        if new_sources:
            existing_sources = existing["sources"]
            if existing_sources:
                # TODO: there might need to be some sort of upstream resource id merging to do past this?
                existing["sources"] = combine_unique_dicts(
                    existing_sources, new_sources, logger
                )
            else:
                existing["sources"] = new_sources

    return og_message


def filter_kgraph_orphans(message, logger: logging.Logger):
    """Given a result-pruned message, filter out orphaned kgraph nodes and edges."""
    try:
        results = message.get("message", {}).get("results", [])
        message_auxgraphs = message.get("message", {}).get("auxiliary_graphs", {})
        kg_edges = (
            message.get("message", {}).get("knowledge_graph", {}).get("edges", {})
        )
        nodes = set()
        edges = set()
        auxgraphs = set()
        temp_auxgraphs = set()
        temp_edges = set()
        # 1. Result node bindings
        for result in results:
            for _, knodes in result.get("node_bindings", {}).items():
                nodes.update([k["id"] for k in knodes])
        # 2. Result.Analysis edge bindings
        for result in results:
            for analysis in result.get("analyses", []):
                for _, kedges in analysis.get("edge_bindings", {}).items():
                    temp_edges.update([k["id"] for k in kedges])
                for _, path_graphs in analysis.get("path_bindings", {}).items():
                    temp_auxgraphs.update(a["id"] for a in path_graphs)
        # 3. Result.Analysis support graphs
        for result in results:
            for analysis in result.get("analyses", []):
                for auxgraph in analysis.get("support_graphs", []):
                    temp_auxgraphs.add(auxgraph)
        # 4. Support graphs from edges in 2
        for edge in temp_edges:
            try:
                edges, auxgraphs, nodes = recursive_get_edge_support_graphs(
                    edge,
                    edges,
                    auxgraphs,
                    kg_edges,
                    message_auxgraphs,
                    nodes,
                )
            except KeyError as e:
                logger.warning(f"Failed to get edge support graph {edge}: {e}")
                continue
        # 5. For all the auxgraphs collect their edges and nodes
        for auxgraph in temp_auxgraphs:
            try:
                edges, auxgraphs, nodes = recursive_get_auxgraph_edges(
                    auxgraph,
                    edges,
                    auxgraphs,
                    kg_edges,
                    message_auxgraphs,
                    nodes,
                )
            except KeyError as e:
                logger.warning(f"Failed to get auxgraph edges {auxgraph}: {e}")
                continue

        # make sure message and knowledge graph exist
        message["message"] = message.get("message") or {}
        message["message"]["knowledge_graph"] = message["message"].get(
            "knowledge_graph"
        ) or {
            "nodes": {},
            "edges": {},
        }
        # now remove all knowledge_graph nodes and edges that are
        # not in our nodes and edges sets.
        kg_nodes = (
            message.get("message", {}).get("knowledge_graph", {}).get("nodes", {})
        )
        message["message"]["knowledge_graph"]["nodes"] = {
            nid: ndata for nid, ndata in kg_nodes.items() if nid in nodes
        }
        kg_edges = (
            message.get("message", {}).get("knowledge_graph", {}).get("edges", {})
        )
        message["message"]["knowledge_graph"]["edges"] = {
            eid: edata for eid, edata in kg_edges.items() if eid in edges
        }
        # validate_message(message)
        message["message"]["auxiliary_graphs"] = {
            auxgraph: adata
            for auxgraph, adata in message["message"]
            .get("auxiliary_graphs", {})
            .items()
            if auxgraph in auxgraphs
        }
        # is_invalid = validate_message(message)
        # if is_invalid:
        #     before_is_invalid = validate_message(initial_message)
        #     if not before_is_invalid:
        #         with open("before_filtering_message.json", "w") as f:
        #             json.dump(initial_message, f, indent=2)
        #         with open("invalid_after_message.json", "w") as f:
        #             json.dump(message, f, indent=2)
        #     else:
        #         print("this message was bad to begin with")
        #         with open("invalid_before_message.json", "w") as f:
        #             json.dump(initial_message, f, indent=2)
    except KeyError as e:
        # can't find the right structure of message
        logger.error(f"Error filtering kgraph orphans: {e}")
        # return message, 400
