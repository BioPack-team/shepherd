"""Aragorn ARA Omnicorp module.

Local port of aragorn-ranker's `omnicorp_overlay.query` (see
https://github.com/ranking-agent/aragorn-ranker/blob/master/ranker/modules/omnicorp_overlay.py).

The upstream module backs `curie_query` / `shared_count_query` with a Redis
cache. Here both lookups are served from local read-only LMDB environments:

* curies LMDB:        key = CURIE (utf-8), value = json-encoded
                      ``{"pmc": <int>, "index": <int>}``.
* shared counts LMDB: key = "<i1>_<i2>" with i1 < i2 (utf-8), value = raw
                      little-endian unsigned int bytes.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from itertools import combinations
from typing import Dict, List
from uuid import uuid4

import lmdb

from shepherd_utils.config import settings
from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, handle_task_failure, wrap_up_task

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

# Matches the upstream `redis_batch_size`.
LMDB_BATCH_SIZE = 1000

# Hot fix: queries with at least this many curie pairs are returned without the
# literature co-occurrence overlay. The per-pair shared-count reads degrade into
# disk-bound random LMDB lookups for very large queries, pushing latency from
# seconds to 45 minutes-3 hours. Override via the env var if needed.
OMNICORP_MAX_CURIE_PAIRS = int(os.environ.get("OMNICORP_MAX_CURIE_PAIRS", 100_000))

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

    Values are raw little-endian unsigned int bytes; decode here so the caller
    receives ints (or ``None`` for misses), matching upstream's ``int(output)``
    expectation.
    """
    out: Dict[str, int] = {}
    with _get_shared_counts_env().begin(buffers=False) as txn:
        for key in keys:
            raw = txn.get(key.encode("utf-8"))
            if raw is None:
                out[key] = None
            else:
                out[key] = int.from_bytes(raw, byteorder="little", signed=False)
    return out


def make_key(x, node_indices):
    """Sorted "index1_index2" key for the shared-counts LMDB."""
    i1 = node_indices[x[0]]
    i2 = node_indices[x[1]]
    if i1 < i2:
        return f"{i1}_{i2}"
    return f"{i2}_{i1}"


def create_log_entry(msg: str, err_level, code=None) -> dict:
    """Build a shepherd-style log entry."""
    now = datetime.now()
    return {
        "timestamp": now.strftime("%m-%d-%Y %H:%M:%S"),
        "level": err_level,
        "message": msg,
        "code": code,
    }


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
    aux_graphs = message["auxiliary_graphs"]
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
                    "attributes": [
                        {
                            "original_attribute_name": "num_publications",
                            "attribute_type_id": "biolink:has_count",
                            "value_type_id": "EDAM:data_0006",
                            "value": publication_count,
                        },
                        {
                            "attribute_type_id": "biolink:agent_type",
                            "value": "statistical_association_pipeline",
                        },
                        {
                            "attribute_type_id": "biolink:knowledge_level",
                            "value": "statistical_association",
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

            if "support_graphs" not in analysis or analysis["support_graphs"] is None:
                analysis["support_graphs"] = []

            omnisupport = None
            for sg in analysis["support_graphs"]:
                if sg.startswith("OMNICORP_support_graph"):
                    omnisupport = sg
                    break

            if omnisupport is None:
                omnisupport = f"OMNICORP_support_graph_{support_idx}"
                support_idx += 1

            analysis["support_graphs"].append(omnisupport)

            if omnisupport not in aux_graphs:
                aux_graphs[omnisupport] = {"edges": [], "attributes": []}

            aux_graphs[omnisupport]["edges"].append(uid)


def generate_curie_pairs(answers, qgraph_setnodes, node_pub_counts, message, logger):
    pair_to_answer = defaultdict(set)

    for ans_idx, answer_map in enumerate(answers):
        nonset_nodes = []
        setnodes = {}

        for nb in answer_map["node_bindings"]:
            if nb in qgraph_setnodes:
                setnodes[nb] = [node["id"] for node in answer_map["node_bindings"][nb]]
            else:
                if len(answer_map["node_bindings"][nb]) != 0:
                    nonset_nodes.extend(
                        [x["id"] for x in answer_map["node_bindings"][nb]]
                    )

        for analysis_idx, analysis in enumerate(answer_map["analyses"]):
            new_nonset_nodes = set()

            relevant_kedge_id_lists = [
                [x["id"] for x in eb] for eb in analysis["edge_bindings"].values()
            ]
            relevant_kedge_ids = [x for el in relevant_kedge_id_lists for x in el]

            auxgraph_ids = []
            for kedge_id in relevant_kedge_ids:
                kedge = message["knowledge_graph"]["edges"][kedge_id]
                for attribute in kedge.get("attributes", []) or []:
                    if attribute["attribute_type_id"] == "biolink:support_graphs":
                        auxgraph_ids.extend(attribute["value"])

            all_relevant_edge_ids = set()
            for auxgraph_id in auxgraph_ids:
                try:
                    all_relevant_edge_ids.update(
                        message["auxiliary_graphs"][auxgraph_id]["edges"]
                    )
                except KeyError:
                    logger.warning(f"Auxgraph id not found: {auxgraph_id}")

            for edge_id in all_relevant_edge_ids:
                try:
                    edge = message["knowledge_graph"]["edges"][edge_id]
                except KeyError:
                    continue

                new_nonset_nodes.add(edge["subject"])
                new_nonset_nodes.add(edge["object"])

            new_nonset_nodes.update(nonset_nodes)
            lookup_nodes = list(new_nonset_nodes)

            lookup_nodes = [n for n in lookup_nodes if n in node_pub_counts]
            lookup_nodes = sorted(lookup_nodes)

            for node_pair in combinations(lookup_nodes, 2):
                pair_to_answer[node_pair].add((ans_idx, analysis_idx))

            for qg_id, snodes in setnodes.items():
                for snode in snodes:
                    for node in lookup_nodes:
                        node_pair = tuple(sorted((node, snode)))
                        pair_to_answer[node_pair].add((ans_idx, analysis_idx))

            for qga, qgb in combinations(setnodes.keys(), 2):
                for anode in setnodes[qga]:
                    for bnode in setnodes[qgb]:
                        node_pair = tuple(sorted((anode, bnode)))
                        pair_to_answer[node_pair].add((ans_idx, analysis_idx))

    return pair_to_answer


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

    if "logs" not in in_message or in_message["logs"] is None:
        in_message["logs"] = []
    else:
        for log in in_message["logs"]:
            log["timestamp"] = str(log["timestamp"])

    message = in_message["message"]
    qgraph = message["query_graph"]
    kgraph = message["knowledge_graph"]
    answers = message["results"]

    if "auxiliary_graphs" not in message or message["auxiliary_graphs"] is None:
        message["auxiliary_graphs"] = {}

    dt_start = datetime.now()

    try:
        start_node_time = datetime.now()

        keys = list(kgraph["nodes"].keys())
        node_pub_counts = {}
        node_indices = {}

        for batch in batches(keys, LMDB_BATCH_SIZE):
            results = curie_query(batch)
            for curie, result in results.items():
                if len(result) == 0:
                    continue
                node_pub_counts[curie] = result["pmc"]
                node_indices[curie] = int(result["index"])

        add_node_pmid_counts(kgraph, node_pub_counts)

        end_node_time = datetime.now()
        logger.info(f"Node time: {end_node_time - start_node_time}")

        start_pair_time = datetime.now()

        qgraph_setnodes = set(
            n
            for n in qgraph["nodes"]
            if (qgraph["nodes"][n].get("set_interpretation", None) or "BATCH")
            != "BATCH"
        )

        t1 = datetime.now()
        pair_to_answer = generate_curie_pairs(
            answers, qgraph_setnodes, node_pub_counts, message, logger
        )
        t2 = datetime.now()
        logger.info(
            f"generate_curie_pairs time: {t2 - t1}. Number of pairs: {len(pair_to_answer)}"
        )

        if len(pair_to_answer) >= OMNICORP_MAX_CURIE_PAIRS:
            logger.warning(
                f"Skipping omnicorp shared-count overlay: {len(pair_to_answer)} "
                f"curie pairs meets/exceeds threshold {OMNICORP_MAX_CURIE_PAIRS}. "
                "Returning message without literature co-occurrence overlay."
            )
            message["knowledge_graph"] = kgraph
            message["results"] = answers
            logger.info("Omnicorp complete. Returning.")
            return in_message

        keypairs = {make_key(x, node_indices): x for x in pair_to_answer.keys()}
        inputkeys = list(keypairs.keys())
        values = {}

        for batch in batches(inputkeys, LMDB_BATCH_SIZE):
            q_start = datetime.now()
            results = shared_count_query(batch)
            q_end = datetime.now()
            logger.debug(
                f"shared_count_query batch ({len(batch)}) time: {q_end - q_start}"
            )

            for input, output in results.items():
                if output is not None:
                    curie_pair = keypairs[input]
                    try:
                        values[curie_pair] = int(output)
                    except Exception:
                        values[curie_pair] = 0

        add_shared_pmid_counts(message, values, pair_to_answer)

        end_pair_time = datetime.now()
        logger.info(f"Pair time: {end_pair_time - start_pair_time}")

        message["knowledge_graph"] = kgraph
        message["results"] = answers

    except Exception as e:
        logger.exception(f"Aragorn-ranker/omnicorp exception {e}")

    if debug == "True":
        diff = datetime.now() - dt_start
        in_message["logs"].append(
            create_log_entry(
                f"End of omnicorp overlay processing. Time elapsed: {diff.seconds} seconds",
                "DEBUG",
            )
        )

    logger.info("Omnicorp complete. Returning.")
    return in_message


def aragorn_omnicorp(message: dict, logger: logging.Logger) -> dict:
    """Run the omnicorp overlay on an already-loaded message.

    CPU-bound; intended to run inside a process pool executor, so it stays
    synchronous. The DB load/save are handled by the async ``process_task``
    on the event loop, not here.
    """
    workflow = None
    if "workflow" in message:
        workflow = message["workflow"]
        del message["workflow"]

    response = omnicorp_overlay(message, logger)

    if workflow is not None:
        response["workflow"] = workflow
    return response


async def process_task(
    task, parent_ctx, logger: logging.Logger, limiter, loop, executor
):
    """Process a given task and ACK in redis.

    The overlay itself is CPU-bound, so it is dispatched to a process pool via
    ``run_in_executor``. That keeps the event loop free to poll for, dispatch,
    and finish other tasks while a single (potentially very large) overlay is
    crunching -- previously the overlay ran inline on the loop and blocked
    everything else until it finished.
    """
    start = time.time()
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        # given a task, get the message from the db (async I/O on the loop)
        response_id = task[1]["response_id"]
        message = await get_message(response_id, logger)
        if message is not None:
            response = await loop.run_in_executor(
                executor,
                aragorn_omnicorp,
                message,
                logger,
            )
            if response is None:
                logger.error("Omnicorp overlay returned nothing. Saving unchanged.")
                response = message
            await save_message(response_id, response, logger)
        else:
            logger.error(f"Failed to get {response_id} for omnicorp overlay.")
        # Always wrap up the task to ACK it in the broker
        try:
            await wrap_up_task(STREAM, GROUP, task, logger)
        except Exception as e:
            logger.error(f"Task {task[0]}: Failed to wrap up task: {e}")
    except asyncio.CancelledError:
        logger.warning(f"Task {task[0]} was cancelled")
    except Exception as e:
        logger.error(f"Task {task[0]} failed with unhandled error: {e}", exc_info=True)
        await handle_task_failure(STREAM, GROUP, task, logger)
    finally:
        span.end()
        limiter.release()
        logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    loop = asyncio.get_running_loop()
    # The overlay is CPU-bound, so cap real parallelism at the number of cores
    # (mirrors aragorn_score). Extra in-flight tasks queue against the pool
    # without blocking the loop.
    cpu_count = os.cpu_count()
    cpu_count = cpu_count if cpu_count is not None else 1
    cpu_count = min(cpu_count, TASK_LIMIT)
    executor = ProcessPoolExecutor(max_workers=cpu_count)
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, TASK_LIMIT
            ):
                asyncio.create_task(
                    process_task(task, parent_ctx, logger, limiter, loop, executor)
                )
        except asyncio.CancelledError:
            logging.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            logging.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
