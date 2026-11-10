"""Aragorn ARA Omnicorp module.

Local port of aragorn-ranker's `omnicorp_overlay.query` (see
https://github.com/ranking-agent/aragorn-ranker/blob/master/ranker/modules/omnicorp_overlay.py).

The upstream module backs `curie_query` / `shared_count_query` with a Redis
cache. Here both lookups are served from local read-only LMDB environments:

* curies LMDB:        key = CURIE (utf-8), value = json-encoded
                      ``{"pmc": <int>, "index": <int>}``.
* shared counts LMDB: key = "<i1>_<i2>" with i1 < i2 (utf-8), value = the
                      count as ASCII decimal text (e.g. ``b"10584"``), carried
                      over verbatim from the upstream Redis string values.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from typing import Dict, List
from uuid import uuid4

import lmdb

from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.data_download import ensure_omnicorp_lmdb
from shepherd_utils.db import get_message_sync, save_response_sync
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import run_task_lifecycle, run_poll_loop
from shepherd_utils.trapi import (
    binding_ids,
    edge_binding_ids,
    edge_support_graphs,
)

# Queue name
STREAM = "aragorn.omnicorp"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
# In-flight task cap. The overlay runs in a process pool (see poll_for_tasks),
# so this bounds how many messages are loaded/queued at once rather than how
# many run in parallel; keep it modest since each message can be large.
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

# Matches the upstream `redis_batch_size`.
LMDB_BATCH_SIZE = 1000

# The overlay is all or nothing, and an overlay that can't finish fails its
# query loudly (#220): there is no curie-pair cap that silently returns a large
# query without literature support, and no partial overlay whose content would
# depend on how fast the disk was. What bounds a large query is time -- the
# shorter of the pool's task timeout and what is left of the query's budget
# (see process_task) -- after which the query ends as TIMEOUT.


#: How a query that ran out of time here is told which step it was.
OVERLAY_STEP = "The literature co-occurrence overlay (omnicorp)"

# Both LMDBs are opened lazily on first use so importing the worker (e.g. in
# tests) does not require the live data files. Static datasets, so we open
# read-only and skip the writer lock.
_curies_env = None
_shared_counts_env = None


def _get_curies_env():
    global _curies_env
    if _curies_env is None:
        _curies_env = lmdb.open(
            settings.omnicorp_curies_lmdb_path,
            subdir=False,
            readonly=True,
            lock=False,
            max_readers=512,
            # These datasets are larger than the pod's memory budget and the
            # reads are effectively random, so the kernel's default readahead
            # just faults in neighbouring pages we never use -- inflating the
            # page cache (and the pod's reported memory) for nothing. Disable
            # it (MDB_NORDAHEAD), matching the score_paths worker.
            readahead=False,
        )
    return _curies_env


def _get_shared_counts_env():
    global _shared_counts_env
    if _shared_counts_env is None:
        _shared_counts_env = lmdb.open(
            settings.omnicorp_shared_counts_lmdb_path,
            subdir=False,
            readonly=True,
            lock=False,
            max_readers=512,
            # See _get_curies_env: random reads over a dataset bigger than RAM,
            # so skip readahead to keep the page cache from ballooning.
            readahead=False,
        )
    return _shared_counts_env


def batches(arr, n):
    """Iterate over arr by batches of size n."""
    for i in range(0, len(arr), n):
        yield arr[i : i + n]


def curie_query(keys: List[str]) -> Dict[str, dict]:
    """LMDB replacement for ``Cache.curie_query``.

    Returns ``{curie: {"pmc": int, "index": int}}`` for hits and ``{curie: {}}``
    for misses, so callers can keep the original ``if len(result) == 0`` check.
    """
    out: Dict[str, dict] = {}
    with _get_curies_env().begin(buffers=False) as txn:
        for key in keys:
            raw = txn.get(key.encode("utf-8"))
            out[key] = json.loads(raw) if raw is not None else {}
    return out


def shared_count_query(keys: List[str]) -> Dict[str, int]:
    """LMDB replacement for ``Cache.shared_count_query``.

    Values are ASCII decimal text; decode here so the caller receives ints
    (or ``None`` for misses), matching upstream's ``int(output)`` expectation.
    Anything that isn't a plain run of digits raises rather than being guessed
    at, so a store in an unexpected format fails loudly instead of emitting
    wrong counts.
    """
    out: Dict[str, int] = {}
    with _get_shared_counts_env().begin(buffers=False) as txn:
        for key in keys:
            raw = txn.get(key.encode("utf-8"))
            if raw is None:
                out[key] = None
            elif raw.isdigit():
                out[key] = int(raw)
            else:
                raise ValueError(
                    f"shared-counts LMDB value for {key!r} is not ASCII decimal: "
                    f"{raw[:32]!r}"
                )
    return out


def make_key(x, node_indices):
    """Sorted "index1_index2" key for the shared-counts LMDB."""
    i1 = node_indices[x[0]]
    i2 = node_indices[x[1]]
    if i1 < i2:
        return f"{i1}_{i2}"
    return f"{i2}_{i1}"


def create_log_entry(msg: str, err_level, code=None) -> dict:
    """Build a TRAPI 2.0 LogEntry (ISO 8601 timestamp; ``code`` omitted, not
    null, when there is none)."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": err_level,
        "message": msg,
    }
    if code is not None:
        entry["code"] = code
    return entry


def add_node_pmid_counts(kgraph, counts):
    for node_id in kgraph["nodes"]:
        if node_id in counts:
            count = counts[node_id]
        else:
            count = 0

        attribute = {
            "original_attribute_name": "omnicorp_article_count",
            "attribute_type_id": "biolink:has_count",
            "value": count,
            "value_type_id": "EDAM:data_0006",
        }

        if (
            "attributes" not in kgraph["nodes"][node_id]
            or kgraph["nodes"][node_id]["attributes"] is None
        ):
            kgraph["nodes"][node_id]["attributes"] = []

        kgraph["nodes"][node_id]["attributes"].append(attribute)


def add_shared_pmid_counts(message, values, pair_to_answer):
    """Count PMIDS shared by a pair of nodes and create a new support edge."""
    kgraph = message["knowledge_graph"]
    answers = message["results"]
    support_idx = 0

    for pair, publication_count in values.items():
        if publication_count == 0:
            continue

        uid = str(uuid4())
        kgraph["edges"].update(
            {
                uid: {
                    "predicate": "biolink:occurs_together_in_literature_with",
                    # TRAPI 2.0: required top-level Edge properties, no longer
                    # biolink:knowledge_level / biolink:agent_type attributes.
                    "knowledge_level": "statistical_association",
                    "agent_type": "statistical_association_pipeline",
                    "attributes": [
                        {
                            "original_attribute_name": "num_publications",
                            "attribute_type_id": "biolink:has_count",
                            "value_type_id": "EDAM:data_0006",
                            "value": publication_count,
                        },
                    ],
                    "sources": [
                        {
                            "resource_id": "infores:omnicorp",
                            "resource_role": "primary_knowledge_source",
                        }
                    ],
                    "subject": pair[0],
                    "object": pair[1],
                }
            }
        )

        for answer_idx, analysis_idx in pair_to_answer[pair]:
            analysis = answers[answer_idx]["analyses"][analysis_idx]

            omnisupport = None
            for sg in analysis.get("support_graphs") or []:
                if sg.startswith("OMNICORP_support_graph"):
                    omnisupport = sg
                    break

            # Only attach the support graph id to the analysis the first time
            # we create it. A single analysis is referenced by many curie pairs
            # (every combination of its nodes maps back to the same analysis),
            # so appending unconditionally added the same OMNICORP_support_graph
            # id to support_graphs once per pair.
            if omnisupport is None:
                omnisupport = f"OMNICORP_support_graph_{support_idx}"
                support_idx += 1
                # TRAPI 2.0: support_graphs has minItems 1, so it is only
                # created here, with its first id.
                if not analysis.get("support_graphs"):
                    analysis["support_graphs"] = []
                analysis["support_graphs"].append(omnisupport)

            # Message.auxiliary_graphs has minProperties 1: create it lazily,
            # with its first graph. AuxiliaryGraph is {"edges": [...]} in 2.0.
            aux_graphs = message.get("auxiliary_graphs")
            if not aux_graphs:
                aux_graphs = message["auxiliary_graphs"] = {}
            if omnisupport not in aux_graphs:
                aux_graphs[omnisupport] = {"edges": []}

            aux_graphs[omnisupport]["edges"].append(uid)


def generate_curie_pairs(
    answers, qgraph_setnodes, node_pub_counts, message, logger, max_pairs=None
):
    """Build the ``{curie_pair: {(answer_idx, analysis_idx)}}`` mapping.

    When ``max_pairs`` is set, generation stops as soon as that many distinct
    pairs have been collected and the (capped) mapping is returned early. The
    overlay itself passes no cap (#220): a query too big to overlay in time
    times out instead.

    A binding or support graph that points at something the message doesn't
    have raises ``ValueError``: the message is malformed, and scoring its
    answers without that evidence would quietly return a different answer.
    """
    pair_to_answer = defaultdict(set)

    for ans_idx, answer_map in enumerate(answers):
        nonset_nodes = []
        setnodes = {}

        # TRAPI 2.0: one NodeBinding {"ids": [...]} per qnode.
        for nb, node_binding in (answer_map.get("node_bindings") or {}).items():
            if nb in qgraph_setnodes:
                setnodes[nb] = binding_ids(node_binding)
            else:
                nonset_nodes.extend(binding_ids(node_binding))

        # Result.analyses is optional in 2.0.
        for analysis_idx, analysis in enumerate(answer_map.get("analyses") or []):
            new_nonset_nodes = set()

            relevant_kedge_ids = edge_binding_ids(analysis)

            auxgraph_ids = []
            for kedge_id in relevant_kedge_ids:
                kedge = message["knowledge_graph"]["edges"].get(kedge_id)
                if kedge is None:
                    raise ValueError(
                        f"Result {ans_idx} binds edge {kedge_id}, which is not "
                        "in the knowledge graph"
                    )
                auxgraph_ids.extend(edge_support_graphs(kedge))

            all_relevant_edge_ids = set()
            aux_graphs = message.get("auxiliary_graphs") or {}
            for auxgraph_id in auxgraph_ids:
                if auxgraph_id not in aux_graphs:
                    raise ValueError(
                        f"Support graph {auxgraph_id} of result {ans_idx} is not "
                        "in the auxiliary graphs"
                    )
                all_relevant_edge_ids.update(aux_graphs[auxgraph_id]["edges"])

            for edge_id in all_relevant_edge_ids:
                edge = message["knowledge_graph"]["edges"].get(edge_id)
                if edge is None:
                    raise ValueError(
                        f"A support graph of result {ans_idx} uses edge "
                        f"{edge_id}, which is not in the knowledge graph"
                    )

                new_nonset_nodes.add(edge["subject"])
                new_nonset_nodes.add(edge["object"])

            new_nonset_nodes.update(nonset_nodes)
            lookup_nodes = list(new_nonset_nodes)

            lookup_nodes = [n for n in lookup_nodes if n in node_pub_counts]
            lookup_nodes = sorted(lookup_nodes)

            for node_pair in combinations(lookup_nodes, 2):
                pair_to_answer[node_pair].add((ans_idx, analysis_idx))
                if max_pairs is not None and len(pair_to_answer) >= max_pairs:
                    return pair_to_answer

            for qg_id, snodes in setnodes.items():
                for snode in snodes:
                    for node in lookup_nodes:
                        node_pair = tuple(sorted((node, snode)))
                        pair_to_answer[node_pair].add((ans_idx, analysis_idx))
                        if max_pairs is not None and len(pair_to_answer) >= max_pairs:
                            return pair_to_answer

            for qga, qgb in combinations(setnodes.keys(), 2):
                for anode in setnodes[qga]:
                    for bnode in setnodes[qgb]:
                        node_pair = tuple(sorted((anode, bnode)))
                        pair_to_answer[node_pair].add((ans_idx, analysis_idx))
                        if max_pairs is not None and len(pair_to_answer) >= max_pairs:
                            return pair_to_answer

    return pair_to_answer


def _already_overlaid(kgraph: dict) -> bool:
    """True if this kgraph already carries omnicorp node annotations.

    ``add_node_pmid_counts`` stamps every kgraph node with an
    ``omnicorp_article_count`` attribute (even when the pair overlay is skipped),
    so its presence means a previous run already applied the overlay. Used to
    stay idempotent: a reclaim can redeliver an already-overlaid message (the
    prior run saved it but died before ACKing), and re-running would double the
    node counts and append duplicate co-occurrence edges with fresh UUIDs.
    """
    for node in kgraph.get("nodes", {}).values():
        for attr in node.get("attributes") or []:
            if attr.get("original_attribute_name") == "omnicorp_article_count":
                return True
    return False


def omnicorp_overlay(in_message: dict, logger: logging.Logger) -> dict:
    """Add literature co-occurrence support to a TRAPI message in-place.

    Mirrors aragorn-ranker's ``omnicorp_overlay.query`` but reads from local
    LMDBs rather than Redis and operates on a plain dict instead of a
    pydantic Response.

    This is CPU-bound (LMDB reads plus pair-building loops with no real I/O
    await points), so it is synchronous and meant to run inside a process
    pool executor rather than directly on the event loop.
    """
    logger.info("Start omnicorp")

    debug = os.environ.get("DEBUG_TIMING", "False")
    if debug == "True":
        dt_start = datetime.now()
        dt_1 = datetime.now()

    dt_2 = datetime.now()
    if debug == "True":
        logger.info(f"convert in message to dict: {dt_2 - dt_1}")

    # Response.logs has minItems 1 in TRAPI 2.0: never create an empty list.
    for log in in_message.get("logs") or []:
        log["timestamp"] = str(log["timestamp"])

    message = in_message["message"]
    qgraph = message["query_graph"]
    kgraph = message["knowledge_graph"]
    answers = message.setdefault("results", [])

    # Idempotency: if this message already carries the omnicorp overlay (a
    # reclaim redelivered a message a previous run had already overlaid and
    # saved), skip -- re-applying would double node counts and stack duplicate
    # co-occurrence edges.
    if _already_overlaid(kgraph):
        logger.info("Omnicorp overlay already applied; skipping to stay idempotent.")
        return in_message

    # auxiliary_graphs is created lazily by add_shared_pmid_counts: an empty
    # {} is invalid TRAPI 2.0 (minProperties 1).
    if message.get("auxiliary_graphs") is None:
        message.pop("auxiliary_graphs", None)

    dt_start = datetime.now()

    # No catch-all here (#220): an overlay that fails -- a malformed message,
    # an unreadable store, a value in the wrong format -- fails the task and
    # its query, rather than returning the answers as if the overlay had run.
    start_node_time = datetime.now()

    keys = sorted(kgraph["nodes"].keys())
    node_pub_counts = {}
    node_indices = {}

    for batch in batches(keys, LMDB_BATCH_SIZE):
        results = curie_query(batch)
        for curie, result in results.items():
            if len(result) == 0:
                continue
            node_pub_counts[curie] = result["pmc"]
            node_indices[curie] = int(result["index"])

    # Node counts are written last, with the pair counts (C6-10): they are
    # what marks a message as overlaid (_already_overlaid), so writing
    # them before a failure part way left a message that no retry would
    # ever finish.

    end_node_time = datetime.now()
    logger.info(f"Node time: {end_node_time - start_node_time}")

    start_pair_time = datetime.now()

    qgraph_setnodes = set(
        n
        for n in qgraph["nodes"]
        if (qgraph["nodes"][n].get("set_interpretation", None) or "BATCH") != "BATCH"
    )

    t1 = datetime.now()
    pair_to_answer = generate_curie_pairs(
        answers,
        qgraph_setnodes,
        node_pub_counts,
        message,
        logger,
    )
    t2 = datetime.now()
    logger.info(
        f"generate_curie_pairs time: {t2 - t1}. Number of pairs: {len(pair_to_answer)}"
    )

    keypairs = {make_key(x, node_indices): x for x in pair_to_answer.keys()}
    inputkeys = sorted(keypairs.keys())
    values = {}

    for batch in batches(inputkeys, LMDB_BATCH_SIZE):
        q_start = datetime.now()
        results = shared_count_query(batch)
        q_end = datetime.now()
        logger.debug(f"shared_count_query batch ({len(batch)}) time: {q_end - q_start}")

        for input, output in results.items():
            if output is not None:
                values[keypairs[input]] = int(output)

    add_node_pmid_counts(kgraph, node_pub_counts)
    add_shared_pmid_counts(message, values, pair_to_answer)

    end_pair_time = datetime.now()
    logger.info(f"Pair time: {end_pair_time - start_pair_time}")

    message["knowledge_graph"] = kgraph
    message["results"] = answers

    if debug == "True":
        diff = datetime.now() - dt_start
        in_message.setdefault("logs", []).append(
            create_log_entry(
                f"End of omnicorp overlay processing. Time elapsed: {diff.seconds} seconds",
                "DEBUG",
            )
        )

    logger.info("Omnicorp complete. Returning.")
    return in_message


def aragorn_omnicorp(response_id: str, logger: logging.Logger) -> None:
    """Process-pool entrypoint: load, overlay, and save entirely in the child.

    Only the small ``response_id`` string crosses the process-pool boundary; the
    (potentially very large) message is read from Redis, mutated, and written
    back inside the child process. Previously the whole message was pickled into
    the child and the mutated copy pickled back out, so a single big query was
    resident several times over -- across the parent event loop, the pickle
    buffers, and the child -- which is what pushed this worker into OOM. Reading
    and writing here keeps the payload off the parent's heap entirely.
    """
    message = get_message_sync(response_id)

    workflow = None
    if "workflow" in message:
        workflow = message["workflow"]
        del message["workflow"]

    response = omnicorp_overlay(message, logger)

    if workflow is not None:
        response["workflow"] = workflow

    save_response_sync(response_id, response)


async def process_task(
    task, parent_ctx, logger: logging.Logger, limiter, loop, pool: ProcessPoolManager
):
    """Process a given task and ACK in redis.

    The overlay itself is CPU-bound, so it is dispatched to a process pool via
    ``run_in_executor``. That keeps the event loop free to poll for, dispatch,
    and finish other tasks while a single (potentially very large) overlay is
    crunching -- previously the overlay ran inline on the loop and blocked
    everything else until it finished. Only the ``response_id`` is handed to the
    child; the message load/save happen there (see ``aragorn_omnicorp``) so the
    payload never crosses the process boundary.

    Dispatch goes through ``pool`` (not a raw executor) so a child dying on an
    oversized message replaces the pool instead of poisoning it for good.
    """

    async def _run(task, logger):
        response_id = task[1]["response_id"]
        # Held no longer than the query can still use the answer (#220,
        # #245): past the query's budget it ends as TIMEOUT.
        await pool.run_within_budget(
            loop, task[1], OVERLAY_STEP, aragorn_omnicorp, response_id, logger
        )

    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, _run)


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    # Ensure the curies / shared-counts LMDBs exist before any pool child tries
    # to open them lazily (a first-run local `docker compose up` starts with the
    # volume-mounted directory empty). No-op once present or when no download URL
    # is configured (e.g. production, where the data is mounted out of band).
    ensure_omnicorp_lmdb(LOGGER)

    loop = asyncio.get_running_loop()
    # The overlay is CPU-bound, so cap real parallelism at the number of cores.
    # Each pool child loads a full (potentially large) message into memory, so
    # this count is also what bounds peak memory -- it MUST reflect the pod's
    # actual CPU allocation, not os.cpu_count() (which reports the whole node's
    # cores and would size the pool for a many-core host, running many big
    # messages at once and OOM-killing the pod). resolve_pool_workers reads the
    # cgroup CPU limit and honours a POOL_MAX_WORKERS override for memory-tight
    # deployments. Extra in-flight tasks queue against the pool without blocking.
    max_workers = resolve_pool_workers(TASK_LIMIT, LOGGER)
    LOGGER.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="aragorn.omnicorp process pool",
        task_timeout=settings.pool_task_timeout_sec,
    )
    await run_poll_loop(
        STREAM,
        GROUP,
        CONSUMER,
        max_workers,
        lambda *task_args: process_task(*task_args, loop, pool),
        on_stop=pool.shutdown,
    )


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
