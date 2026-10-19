"""Aragorn ARA Pathfinder module."""

import asyncio
import json
import logging
import os
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import pairwise
from math import isfinite
from pathlib import Path
from typing import Optional

import httpx
import numpy as np
from opentelemetry.propagate import inject

from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.data_download import ensure_pathfinder_search_graph
from shepherd_utils.db import (
    abandon_lookup_if_data_lost,
    add_callback_id,
    cleanup_callbacks,
    get_message,
    get_message_sync,
    get_running_callbacks,
    remove_callback_id,
    save_response,
    save_response_sync,
)
from shepherd_utils.logger import QueryLogger, get_query_handler, get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import (
    filter_kgraph_orphans,
    get_tasks,
    run_task_lifecycle,
)
from shepherd_utils.trapi import make_binding

# Queue name
STREAM = "aragorn.pathfinder"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

# The backbone's pair graph as a CSR (offsets, neighbors, weights) plus its node
# CURIEs in index order, written by squashbert's scripts/export_search_graph.py.
SEARCH_GRAPH_DIR = settings.pathfinder_search_graph_dir
MAX_PATHS = 12000
SEARCH_BUDGET_SEC = 5.0
MIDDLE_CHUNK = 4096
# Each lookup holds a Postgres connection while it registers its callback; half
# the pool leaves the rest for the callback wait and the other queries.
MAX_LOOKUPS_IN_FLIGHT = max(1, settings.postgres_pool_max_size // 2)
SOURCE = "infores:shepherd-aragorn"
ONTOLOGY_PREDICATES = {"biolink:subclass_of", "biolink:superclass_of"}

# Set up in poll_for_tasks.
NODE_CURIES: list[str] = []
NODE_INDEX: dict[str, int] = {}
POOL: Optional[ProcessPoolManager] = None
# Per-child CSR, memory-mapped on first use so the pool children share one copy.
_search_graphs: dict = {}


@dataclass
class AsyncResponse:
    status_code: int
    success: bool
    callback_id: str
    error: Optional[str] = None
    # True when the submission was sent but we never heard the ACK. The
    # retrieval service may well be running the lookup and will POST the
    # callback later, so the callback id must be kept.
    in_flight: bool = False


def load_search_graph(graph_dir: str):
    """The search graph's CSR arrays, memory-mapped read-only."""
    if graph_dir not in _search_graphs:
        _search_graphs[graph_dir] = tuple(
            np.load(Path(graph_dir) / f"{name}.npy", mmap_mode="r")
            for name in ("offsets", "neighbors", "weights")
        )
    return _search_graphs[graph_dir]


def cheapest_legs(graph, start, far):
    """The one-hop leg and the two cheapest two-hop legs, through different
    intermediates, from ``start`` to every node.

    A leg never passes through or ends at ``start`` or ``far``. ``via`` is
    each two-hop leg's intermediate.
    """
    offsets, neighbors, weights = graph
    num_nodes = len(offsets) - 1

    def gather(rows):
        lo = np.asarray(offsets[rows], dtype=np.int64)
        counts = np.asarray(offsets[rows + 1], dtype=np.int64) - lo
        ends = np.cumsum(counts)
        slots = np.arange(int(counts.sum())) + np.repeat(lo - (ends - counts), counts)
        return slots, counts

    slots, _ = gather(np.array([start]))
    heads = np.asarray(neighbors[slots], dtype=np.int64)
    cost = np.asarray(weights[slots], dtype=np.float64)
    kept = np.isfinite(cost) & (heads != far) & (heads != start)
    heads, cost = heads[kept], cost[kept]
    one = np.full(num_nodes, np.inf)
    np.minimum.at(one, heads, cost)

    first = np.unique(heads)
    slots, counts = gather(first)
    via_all = np.repeat(first, counts)
    heads = np.asarray(neighbors[slots], dtype=np.int64)
    cost = np.repeat(one[first], counts) + np.asarray(weights[slots], dtype=np.float64)
    kept = (heads != start) & (heads != far) & (heads != via_all) & np.isfinite(cost)
    via_all, heads, cost = via_all[kept], heads[kept], cost[kept]

    two = np.full((2, num_nodes), np.inf)
    via = np.full((2, num_nodes), -1, dtype=np.int64)
    for rank in range(2):
        np.minimum.at(two[rank], heads, cost)
        at = np.flatnonzero(np.isfinite(cost) & (cost == two[rank][heads]))
        if not len(at):
            break
        _, first_at = np.unique(heads[at], return_index=True)
        chosen = at[first_at]
        via[rank][heads[chosen]] = via_all[chosen]
        cost[chosen] = np.inf
    return one, two, via


def search_paths(graph_dir, source, target, max_paths):
    """Process-pool entrypoint: middle-first paths between two backbone nodes.

    Every node within two hops of both ends is priced at its cheapest loopless
    path with that node at the centre, and the middles come out in that order,
    each with that one path. A three-hop path is centred on both its
    intermediates and comes out once.
    """
    deadline = time.monotonic() + SEARCH_BUDGET_SEC
    graph = load_search_graph(graph_dir)
    one_s, two_s, via_s = cheapest_legs(graph, source, target)
    one_t, two_t, via_t = cheapest_legs(graph, target, source)
    # The cheapest leg out and back can share their intermediate; the runner-up
    # on either side prices the middle loopless.
    both_two = np.where(
        via_s[0] != via_t[0],
        two_s[0] + two_t[0],
        np.minimum(two_s[0] + two_t[1], two_s[1] + two_t[0]),
    )
    score = np.minimum.reduce(
        [one_s + one_t, one_s + two_t[0], two_s[0] + one_t, both_two]
    )
    middles = np.flatnonzero(np.isfinite(score))
    order = middles[np.argsort(score[middles], kind="stable")]

    paths = []
    seen = set()
    for chunk in range(0, len(order), MIDDLE_CHUNK):
        if time.monotonic() > deadline:
            break
        ms = order[chunk : chunk + MIDDLE_CHUNK]
        no_via = np.full(len(ms), -1)
        f_cost = np.column_stack([one_s[ms], two_s[0][ms], two_s[1][ms]])
        f_via = np.column_stack([no_via, via_s[0][ms], via_s[1][ms]])
        b_cost = np.column_stack([one_t[ms], two_t[0][ms], two_t[1][ms]])
        b_via = np.column_stack([no_via, via_t[0][ms], via_t[1][ms]])
        pairs = f_cost[:, :, None] + b_cost[:, None, :]
        pairs[(f_via[:, :, None] == b_via[:, None, :]) & (f_via[:, :, None] >= 0)] = (
            np.inf
        )
        pairs = pairs.reshape(len(ms), 9)
        picks = pairs.argmin(axis=1)
        costs = pairs[np.arange(len(ms)), picks]
        for i, m in enumerate(ms.tolist()):
            if not isfinite(costs[i]):
                continue
            a = int(f_via[i, picks[i] // 3])
            b = int(b_via[i, picks[i] % 3])
            nodes = (
                source,
                *((a,) if a >= 0 else ()),
                m,
                *((b,) if b >= 0 else ()),
                target,
            )
            if len(nodes) == 4:
                if nodes in seen:
                    continue
                seen.add(nodes)
            paths.append(nodes)
            if len(paths) >= max_paths:
                return paths, len(order)
    return paths, len(order)


def one_hop(left, right, parameters, subclass):
    """A one-hop lookup between two query nodes, each pinned to ids or open."""
    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {} if left is None else {"ids": list(left)},
                    "n1": {} if right is None else {"ids": list(right)},
                },
                "edges": {
                    "e0": {
                        "subject": "n0",
                        "object": "n1",
                        "predicates": ["biolink:related_to"],
                    }
                },
            }
        },
        "parameters": {**parameters, "subclass": subclass},
    }


def edge_lookups(node_paths, source, target, parameters):
    """One-hop lookups for every hop the paths take, and for the direct edge.

    Gandalf expands the endpoints to their subclass children: the direct edge,
    and each endpoint against the nodes the paths take next to it, with
    ``subclass_qnodes`` keeping those nodes exact. Against anything, a hub
    endpoint returns every edge of it and its children. The hops between
    intermediates are exact, pinned in one
    lookup with every near node on one side and every far node on the other:
    one lookup per hop would be thousands of lookups, each with its callback
    to register and merge. Its ``pairs`` hold gandalf to the hops themselves
    rather than the edges between every near and every far node; a server
    that ignores them returns those too, which no path uses and the orphan
    filter drops.
    """
    lookups = [one_hop([source], [target], parameters, True)]
    if node_paths:
        first = sorted({path[1] for path in node_paths})
        last = sorted({path[-2] for path in node_paths})
        lookups.append(
            one_hop([source], first, {**parameters, "subclass_qnodes": ["n0"]}, True)
        )
        lookups.append(
            one_hop(last, [target], {**parameters, "subclass_qnodes": ["n1"]}, True)
        )
    hops = set()
    for path in node_paths:
        if len(path) > 3:
            hops.add((path[1], path[2]))
        if len(path) > 4:
            hops.add((path[-2], path[-3]))
    if hops:
        near = sorted({u for u, _ in hops})
        far = sorted({v for _, v in hops})
        pairs = [list(hop) for hop in sorted(hops)]
        lookups.append(one_hop(near, far, {**parameters, "pairs": pairs}, False))
    return lookups


async def run_async_lookup(
    client: httpx.AsyncClient,
    message: dict,
    query_id: str,
    logger: logging.Logger,
) -> AsyncResponse:
    """Return an async lookup response with callback id."""
    callback_id = str(uuid.uuid4())[:8]
    with tracer.start_as_current_span("aragorn.pathfinder.lookup") as span:
        span.set_attribute("callback.id", callback_id)
        lookup_carrier = {}
        inject(lookup_carrier)
        await add_callback_id(query_id, callback_id, json.dumps(lookup_carrier), logger)

        message["callback"] = f"{settings.callback_host}/aragorn/callback/{callback_id}"
        logger.debug(
            f"[{callback_id}] Sending edge lookup to {settings.kg_retrieval_url}"
        )
        try:
            response = await client.post(settings.kg_retrieval_url, json=message)
            return AsyncResponse(
                status_code=response.status_code,
                success=response.status_code == 200,
                callback_id=callback_id,
            )
        except httpx.ReadTimeout as e:
            span.record_exception(e)
            return AsyncResponse(
                status_code=500,
                success=False,
                callback_id=callback_id,
                error=f"{type(e).__name__}: no ACK within {client.timeout.read}s",
                in_flight=True,
            )
        except Exception as e:
            span.record_exception(e)
            return AsyncResponse(
                status_code=500,
                success=False,
                callback_id=callback_id,
                error=f"{type(e).__name__}: {e}",
            )


async def send_lookups(lookups, query_id, logger: logging.Logger):
    """Send the lookups, ``MAX_LOOKUPS_IN_FLIGHT`` at a time, dropping the
    callbacks of any that failed."""
    in_flight = asyncio.Semaphore(MAX_LOOKUPS_IN_FLIGHT)

    async def send(client, lookup):
        async with in_flight:
            return await run_async_lookup(client, lookup, query_id, logger)

    async with httpx.AsyncClient(
        timeout=settings.kg_retrieval_submit_timeout
    ) as client:
        responses = await asyncio.gather(
            *(send(client, lookup) for lookup in lookups),
            return_exceptions=True,
        )
    for response in responses:
        if isinstance(response, Exception):
            logger.error(
                f"Failed to do lookup and unable to remove callback id: {response}"
            )
        elif not response.success:
            if response.in_flight:
                logger.warning(
                    f"[{response.callback_id}] Lookup submitted but not "
                    f"acknowledged, waiting for its callback anyway: {response.error}"
                )
                continue
            logger.error(
                f"[{response.callback_id}] Failed to do lookup, removing callback "
                f"id: {response.error}"
            )
            await remove_callback_id(response.callback_id, logger)


async def wait_for_callbacks(
    query_id, response_id, max_query_time, logger: logging.Logger
):
    """Wait until every lookup's callback is merged, or the query times out."""
    start_time = time.time()
    running_callback_ids = [""]
    try:
        while time.time() - start_time < max_query_time:
            try:
                running_callback_ids = await get_running_callbacks(query_id, logger)
            except Exception:
                # Brief backoff then retry the check rather than giving up
                await asyncio.sleep(5)
                continue
            # fail now if the query data the callbacks merge into is gone
            await abandon_lookup_if_data_lost(
                query_id, response_id, running_callback_ids, logger
            )
            if len(running_callback_ids) > 0:
                await asyncio.sleep(1)
                continue
            logger.debug("Got all lookups back. Continuing...")
            break

        if time.time() - start_time > max_query_time:
            logger.warning(
                f"Timed out getting lookup callbacks. {len(running_callback_ids)} queries were still running..."
            )
            logger.warning(f"Running callbacks: {running_callback_ids}")
            await cleanup_callbacks(query_id, logger)
    except asyncio.CancelledError:
        logger.warning("Cancelled while waiting for callbacks")


def build_paths(message, plan, logger: logging.Logger):
    """Turn the search's node paths into TRAPI paths over the looked-up edges.

    A hop is every edge between its two nodes. At an endpoint that includes
    gandalf's inferred edges from the endpoint's subclass children, each
    supported by the child's edge and its subclass_of edge. A path with a hop
    that came back empty is dropped.
    """
    kgraph = message.get("knowledge_graph") or {"nodes": {}, "edges": {}}
    nodes, edges = kgraph["nodes"], kgraph["edges"]
    aux_graphs = message.get("auxiliary_graphs") or {}
    source, target = plan["source"], plan["target"]

    by_pair = defaultdict(list)
    for edge_id, edge in edges.items():
        if edge["predicate"] not in ONTOLOGY_PREDICATES:
            by_pair[frozenset((edge["subject"], edge["object"]))].append(edge_id)

    def path_edges(path):
        """The path's edges, or None and the first hop that has none."""
        found = []
        for hop, (u, v) in enumerate(pairwise(path)):
            hop_edges = by_pair.get(frozenset((u, v)))
            if not hop_edges:
                return None, hop
            found.extend(hop_edges)
        return list(dict.fromkeys(found)), None

    category = plan["intermediate_category"]
    analyses = []
    seen = set()
    dropped = Counter()
    first_missing = None
    # The direct edge is the one-hop path, first.
    for index, path in enumerate([[source, target], *plan["node_paths"]]):
        intermediates = tuple(path[1:-1])
        if intermediates in seen:
            continue
        seen.add(intermediates)
        if category is not None and category != "biolink:NamedThing":
            if not any(
                category in (nodes.get(node, {}).get("categories") or [])
                for node in intermediates
            ):
                continue
        edge_ids, missing = path_edges(path)
        if edge_ids is None:
            if len(path) == 2:
                dropped["direct"] += 1
            elif missing == 0:
                dropped["at the source"] += 1
            elif missing == len(path) - 2:
                dropped["at the target"] += 1
            else:
                dropped["between intermediates"] += 1
            first_missing = first_missing or (path[missing], path[missing + 1])
            continue
        aux_id = f"p_{index}"
        aux_graphs[aux_id] = {"edges": edge_ids}
        analyses.append(
            {
                "resource_id": SOURCE,
                "path_bindings": {plan["path_key"]: make_binding([aux_id])},
            }
        )
    logger.info(
        f"Built {len(analyses)} paths from {len(edges)} edges; dropped "
        f"{dropped.total()} with a hop that has no edges {dict(dropped)}."
    )
    if first_missing:
        # Whether the hop's nodes are in the graph at all tells ids that do not
        # match it apart from a pair it holds no edge between.
        linked = set().union(*by_pair)
        u, v = first_missing
        logger.info(
            f"First hop with no edges: {u} ({'in' if u in linked else 'not in'} "
            f"the edges) - {v} ({'in' if v in linked else 'not in'} the edges)."
        )

    message["knowledge_graph"] = kgraph
    message["results"] = []
    if analyses:
        message["auxiliary_graphs"] = aux_graphs
        message["results"] = [
            {
                "node_bindings": {
                    plan["subject_key"]: make_binding([source]),
                    plan["object_key"]: make_binding([target]),
                },
                "analyses": analyses,
            }
        ]


def build_paths_task(response_id: str, plan: dict, log_level: int) -> list[dict]:
    """Process-pool entrypoint: build the paths into the merged response and
    save it, returning this child's log records for the parent."""
    query_log_handler = QueryLogger().log_handler
    logger = get_worker_logger(f"{STREAM}.worker.{os.getpid()}")
    logger.setLevel(log_level)
    logger.addHandler(query_log_handler)
    try:
        response = get_message_sync(response_id)
        build_paths(response["message"], plan, logger)
        filter_kgraph_orphans(response, logger)
        save_response_sync(response_id, response)
        return query_log_handler.drain()
    finally:
        logger.removeHandler(query_log_handler)


async def shadowfax(task, logger: logging.Logger) -> str:
    """Processes pathfinder queries. Middle-first search over the backbone
    finds the paths as nodes, retriever returns the edges for every hop they
    take, and once all of those lookups are merged the paths are built from
    them."""
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    response_id = task[1]["response_id"]
    metadata = json.loads(task[1]["metadata"])
    message = await get_message(query_id, logger)
    parameters = message.get("parameters") or {}
    parameters["timeout"] = parameters.get("timeout", settings.lookup_timeout)

    if metadata.get("rehydrate"):
        # do rehydration
        response = await get_message(response_id, logger)
        parameters["rehydrate"] = parameters.get("rehydrate", True)
        response["parameters"] = parameters
        logger.debug(
            f"""Sending pathfinder rehydration to {settings.kg_rehydrate_url}."""
        )
        with tracer.start_as_current_span("aragorn.pathfinder.rehydrate") as span:
            span.set_attribute("query.id", query_id)
            async with httpx.AsyncClient(timeout=210) as client:
                # send a sync rehydrate query that "should" be very quick
                rehydrated_response = await client.post(
                    settings.kg_rehydrate_url,
                    json=response,
                )
                rehydrated_response.raise_for_status()
                response_json = rehydrated_response.json()
                await save_response(response_id, response_json, logger)
                return json.dumps({})

    qgraph = message["message"]["query_graph"]
    pinned_node_ids = [
        node["ids"][0] for node in qgraph["nodes"].values() if node.get("ids")
    ]
    if len(set(pinned_node_ids)) != 2:
        raise Exception("Pathfinder queries require two pinned nodes.")
    path_key, qpath = next(iter(qgraph["paths"].items()))
    constraints = qpath.get("constraints") or []
    if len(constraints) > 1:
        raise Exception("Pathfinder queries do not support multiple constraints.")
    intermediate_categories = (
        (constraints[0].get("required_intermediate_categories") or [])
        if constraints
        else []
    )
    if len(intermediate_categories) > 1:
        raise Exception(
            "Pathfinder queries do not support multiple intermediate categories"
        )
    source = qgraph["nodes"][qpath["subject"]]["ids"][0]
    target = qgraph["nodes"][qpath["object"]]["ids"][0]

    lookup_parameters = {
        "timeout": parameters["timeout"],
        "tiers": [settings.default_data_tier],
        # tier 1 would expand every pinned node and cannot dehydrate
        "tier_fallback": False,
        "dehydrated": True,
        # the paths are built from the knowledge graph alone, and merging
        # drops a lookup's results anyway
        "kg_only": True,
    }

    node_paths = []
    if source in NODE_INDEX and target in NODE_INDEX:
        with tracer.start_as_current_span("aragorn.pathfinder.search") as span:
            paths, middles = await POOL.run(
                asyncio.get_running_loop(),
                search_paths,
                SEARCH_GRAPH_DIR,
                NODE_INDEX[source],
                NODE_INDEX[target],
                MAX_PATHS,
            )
            span.set_attribute("paths", len(paths))
        node_paths = [[NODE_CURIES[node] for node in path] for path in paths]
        logger.info(
            f"Backbone search found {len(node_paths)} paths over {middles} middles."
        )
    else:
        logger.warning(
            f"{source} or {target} is not on the backbone; "
            "looking up the direct edge only."
        )

    lookups = edge_lookups(node_paths, source, target, lookup_parameters)
    logger.info(f"Sending {len(lookups)} edge lookups to {settings.kg_retrieval_url}")
    await send_lookups(lookups, query_id, logger)
    await wait_for_callbacks(query_id, response_id, parameters["timeout"], logger)

    plan = {
        "source": source,
        "target": target,
        "node_paths": node_paths,
        "path_key": path_key,
        "subject_key": qpath["subject"],
        "object_key": qpath["object"],
        "intermediate_category": (
            intermediate_categories[0] if intermediate_categories else None
        ),
    }
    with tracer.start_as_current_span("aragorn.pathfinder.build_paths"):
        entries = await POOL.run(
            asyncio.get_running_loop(),
            build_paths_task,
            response_id,
            plan,
            logger.getEffectiveLevel(),
        )
    handler = get_query_handler(logger)
    if handler is not None and entries:
        handler.ingest(entries)

    return json.dumps({"rehydrate": True})


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""

    async def _run(task, logger):
        metadata = await shadowfax(task, logger)
        task[1]["metadata"] = metadata

    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, _run)


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    global NODE_CURIES, NODE_INDEX, POOL
    # In the parent, before any pool child maps the files.
    ensure_pathfinder_search_graph(LOGGER)
    NODE_CURIES = (Path(SEARCH_GRAPH_DIR) / "nodes.txt").read_text().splitlines()
    NODE_INDEX = {curie: index for index, curie in enumerate(NODE_CURIES)}
    offsets, _, _ = load_search_graph(SEARCH_GRAPH_DIR)
    if len(offsets) != len(NODE_CURIES) + 1:
        raise RuntimeError(
            f"{SEARCH_GRAPH_DIR} holds {len(offsets) - 1} nodes in its CSR but "
            f"{len(NODE_CURIES)} in nodes.txt"
        )
    LOGGER.info(f"{STREAM}: search graph over {len(NODE_CURIES)} nodes.")
    # The search and the path building are CPU-bound; the waits between them
    # are not, so more tasks run than there are pool workers.
    max_workers = resolve_pool_workers(TASK_LIMIT, LOGGER)
    LOGGER.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    POOL = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="aragorn.pathfinder process pool",
        task_timeout=settings.pool_task_timeout_sec,
    )
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
