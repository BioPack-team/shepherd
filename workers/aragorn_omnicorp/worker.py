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
import gc
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from itertools import combinations
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4

import lmdb
import numpy as np

from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.data_download import ensure_omnicorp_lmdb
from shepherd_utils.db import get_message_sync, save_response_sync
from shepherd_utils.logger import (
    get_worker_logger,
    ingest_query_logs,
    pool_task_logger,
)
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


def read_shared_counts(keys: Sequence[bytes]) -> List[Optional[int]]:
    """Look up shared counts for ``keys`` (utf-8 ``b"<i1>_<i2>"``), in order.

    Returns the count as an int, or ``None`` for a miss. All reads share one
    read transaction, and callers should pass the keys in sorted (byte)
    order: that is LMDB's key order, so neighbouring lookups land on
    neighbouring pages of a store that is bigger than RAM.

    Values are ASCII decimal text. Anything that isn't a plain run of digits
    raises rather than being guessed at, so a store in an unexpected format
    fails loudly instead of emitting wrong counts.
    """
    with _get_shared_counts_env().begin(buffers=False) as txn:
        get = txn.get
        raws = [get(key) for key in keys]
    out: List[Optional[int]] = []
    append = out.append
    for key, raw in zip(keys, raws):
        if raw is None:
            append(None)
        elif raw.isdigit():
            append(int(raw))
        else:
            raise ValueError(
                f"shared-counts LMDB value for {key.decode('utf-8', 'replace')!r} "
                f"is not ASCII decimal: {raw[:32]!r}"
            )
    return out


def shared_count_query(keys: List[str]) -> Dict[str, Optional[int]]:
    """LMDB replacement for ``Cache.shared_count_query``.

    Returns ``{key: count}``, with ``None`` for misses, matching upstream's
    ``int(output)`` expectation. See ``read_shared_counts``.
    """
    counts = read_shared_counts([key.encode("utf-8") for key in keys])
    return dict(zip(keys, counts))


def shared_count_keys(index_a: np.ndarray, index_b: np.ndarray) -> List[bytes]:
    """Shared-counts LMDB keys ``b"<i1>_<i2>"`` (``i1 < i2``) for each pair of
    omnicorp indices."""
    low = np.minimum(index_a, index_b).tolist()
    high = np.maximum(index_a, index_b).tolist()
    return [f"{i1}_{i2}".encode() for i1, i2 in zip(low, high)]


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


def add_shared_pmid_counts(
    message: dict,
    cooccurrences: Iterable[Tuple[str, str, int, Iterable[Tuple[int, int]]]],
) -> None:
    """Create a co-occurrence support edge for each pair of nodes that share
    publications, and attach it to the analyses the pair came from.

    ``cooccurrences`` yields ``(subject, object, publication_count, refs)``,
    where ``refs`` are the ``(result_idx, analysis_idx)`` of the analyses the
    pair supports. Each analysis gets one OMNICORP support graph (reusing one
    it already has), which collects every co-occurrence edge for it.
    """
    kgraph_edges = message["knowledge_graph"]["edges"]
    answers = message["results"]
    support_idx = 0
    # (result_idx, analysis_idx) -> its OMNICORP aux graph's edge list. A
    # single analysis is referenced by many curie pairs (every combination of
    # its nodes), so resolve its support graph once rather than per pair.
    analysis_edges: Dict[Tuple[int, int], list] = {}

    for subject, obj, publication_count, refs in cooccurrences:
        if publication_count == 0:
            continue

        uid = str(uuid4())
        kgraph_edges[uid] = {
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
            "subject": subject,
            "object": obj,
        }

        for ref in refs:
            edges = analysis_edges.get(ref)
            if edges is None:
                answer_idx, analysis_idx = ref
                analysis = answers[answer_idx]["analyses"][analysis_idx]

                omnisupport = None
                for sg in analysis.get("support_graphs") or []:
                    if sg.startswith("OMNICORP_support_graph"):
                        omnisupport = sg
                        break

                if omnisupport is None:
                    omnisupport = f"OMNICORP_support_graph_{support_idx}"
                    support_idx += 1
                    # TRAPI 2.0: support_graphs has minItems 1, so it is only
                    # created here, with its first id.
                    if not analysis.get("support_graphs"):
                        analysis["support_graphs"] = []
                    analysis["support_graphs"].append(omnisupport)

                # Message.auxiliary_graphs has minProperties 1: create it
                # lazily, with its first graph. AuxiliaryGraph is
                # {"edges": [...]} in 2.0.
                aux_graphs = message.get("auxiliary_graphs")
                if not aux_graphs:
                    aux_graphs = message["auxiliary_graphs"] = {}
                if omnisupport not in aux_graphs:
                    aux_graphs[omnisupport] = {"edges": []}
                edges = analysis_edges[ref] = aux_graphs[omnisupport]["edges"]

            edges.append(uid)


class CuriePairs:
    """The distinct curie pairs to look up, and the analyses each supports.

    Pairs are held as numpy arrays over local node ids rather than as a dict
    of curie-tuple keys to sets of ``(result_idx, analysis_idx)`` tuples: a
    big creative query yields millions of pairs and many more (pair, analysis)
    links, and as Python objects those dominated both the overlay's time
    (allocation plus the cyclic GC repeatedly scanning them) and its memory.

    * ``first`` / ``second``: the local ids of each distinct pair, with
      ``first < second`` (so the curies are in sorted order).
    * ``refs[starts[i]:ends[i]]``: indexes into ``analyses`` (a list of
      ``(result_idx, analysis_idx)``) of the analyses pair ``i`` supports.
    """

    def __init__(self, codes: np.ndarray, refs: np.ndarray, num_nodes: int, analyses):
        # Distinct (pair, analysis) links, grouped by pair.
        order = np.lexsort((refs, codes))
        codes, refs = codes[order], refs[order]
        if len(codes):
            keep = np.empty(len(codes), dtype=bool)
            keep[0] = True
            keep[1:] = (codes[1:] != codes[:-1]) | (refs[1:] != refs[:-1])
            codes, refs = codes[keep], refs[keep]
        pairs, starts = np.unique(codes, return_index=True)
        self.first = pairs // max(num_nodes, 1)
        self.second = pairs % max(num_nodes, 1)
        self.starts = starts
        self.ends = np.append(starts[1:], len(codes)) if len(starts) else starts
        self.refs = refs
        self.analyses = analyses

    def __len__(self) -> int:
        return len(self.first)

    def pair_refs(self, i: int) -> List[Tuple[int, int]]:
        """The ``(result_idx, analysis_idx)`` of every analysis pair ``i`` supports."""
        analyses = self.analyses
        return [analyses[k] for k in self.refs[self.starts[i] : self.ends[i]].tolist()]


def generate_curie_pairs(
    answers, qgraph_setnodes, node_ids: Dict[str, int], message
) -> CuriePairs:
    """Collect every curie pair the overlay looks up, and the analyses each
    pair supports.

    ``node_ids`` maps each curie with omnicorp data to a local id, assigned in
    sorted curie order; curies without data have no co-occurrence counts, so
    pairs involving them are never generated. That holds for nodes bound to a
    set-interpretation qnode too: those were once paired unfiltered, and a
    curie missing from the curies LMDB then failed the whole query with a
    ``KeyError`` when its pair key was built.

    A binding or support graph that points at something the message doesn't
    have raises ``ValueError``: the message is malformed, and scoring its
    answers without that evidence would quietly return a different answer.
    """
    num_nodes = len(node_ids)
    kgraph_edges = message["knowledge_graph"]["edges"]
    aux_graphs = message.get("auxiliary_graphs") or {}
    analyses: List[Tuple[int, int]] = []
    code_chunks = []
    ref_chunks = []

    for ans_idx, answer_map in enumerate(answers):
        nonset_nodes = []
        setnodes = {}

        # TRAPI 2.0: one NodeBinding {"ids": [...]} per qnode.
        for nb, node_binding in (answer_map.get("node_bindings") or {}).items():
            if nb in qgraph_setnodes:
                setnodes[nb] = [
                    node_ids[n] for n in binding_ids(node_binding) if n in node_ids
                ]
            else:
                nonset_nodes.extend(binding_ids(node_binding))

        # Result.analyses is optional in 2.0.
        for analysis_idx, analysis in enumerate(answer_map.get("analyses") or []):
            analysis_nodes = set(nonset_nodes)

            for kedge_id in edge_binding_ids(analysis):
                kedge = kgraph_edges.get(kedge_id)
                if kedge is None:
                    raise ValueError(
                        f"Result {ans_idx} binds edge {kedge_id}, which is not "
                        "in the knowledge graph"
                    )
                for auxgraph_id in edge_support_graphs(kedge):
                    if auxgraph_id not in aux_graphs:
                        raise ValueError(
                            f"Support graph {auxgraph_id} of result {ans_idx} is "
                            "not in the auxiliary graphs"
                        )
                    for edge_id in aux_graphs[auxgraph_id]["edges"]:
                        edge = kgraph_edges.get(edge_id)
                        if edge is None:
                            raise ValueError(
                                f"A support graph of result {ans_idx} uses edge "
                                f"{edge_id}, which is not in the knowledge graph"
                            )
                        analysis_nodes.add(edge["subject"])
                        analysis_nodes.add(edge["object"])

            lookup = sorted(node_ids[n] for n in analysis_nodes if n in node_ids)

            # Pair (a, b), a < b, is encoded as the int a * num_nodes + b.
            codes = [a * num_nodes + b for a, b in combinations(lookup, 2)]
            for snodes in setnodes.values():
                for s in snodes:
                    codes.extend(
                        (n * num_nodes + s) if n < s else (s * num_nodes + n)
                        for n in lookup
                        if n != s
                    )
            for qga, qgb in combinations(setnodes, 2):
                for a in setnodes[qga]:
                    codes.extend(
                        (a * num_nodes + b) if a < b else (b * num_nodes + a)
                        for b in setnodes[qgb]
                        if a != b
                    )

            if codes:
                code_chunks.append(np.array(codes, dtype=np.int64))
                ref_chunks.append(np.full(len(codes), len(analyses), dtype=np.int64))
            analyses.append((ans_idx, analysis_idx))

    if code_chunks:
        codes = np.concatenate(code_chunks)
        refs = np.concatenate(ref_chunks)
    else:
        codes = refs = np.empty(0, dtype=np.int64)
    return CuriePairs(codes, refs, num_nodes, analyses)


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

    # Local ids in sorted curie order (``keys`` is sorted), so a pair's lower
    # id is its lexically smaller curie: the edge's subject, as before.
    curies = [curie for curie in keys if curie in node_indices]
    local_ids = {curie: i for i, curie in enumerate(curies)}
    omnicorp_index = np.array([node_indices[c] for c in curies], dtype=np.int64)

    t1 = datetime.now()
    pairs = generate_curie_pairs(answers, qgraph_setnodes, local_ids, message)
    t2 = datetime.now()
    logger.info(f"generate_curie_pairs time: {t2 - t1}. Number of pairs: {len(pairs)}")

    lmdb_keys = shared_count_keys(
        omnicorp_index[pairs.first], omnicorp_index[pairs.second]
    )
    # Look up in LMDB key order (see read_shared_counts); the co-occurrence
    # edges are created in that order too, as they always have been.
    key_order = sorted(range(len(lmdb_keys)), key=lmdb_keys.__getitem__)
    q_start = datetime.now()
    counts = read_shared_counts([lmdb_keys[i] for i in key_order])
    del lmdb_keys
    logger.info(
        f"shared count lookups ({len(counts)}) time: {datetime.now() - q_start}"
    )

    first = pairs.first.tolist()
    second = pairs.second.tolist()

    add_node_pmid_counts(kgraph, node_pub_counts)
    add_shared_pmid_counts(
        message,
        (
            (curies[first[i]], curies[second[i]], count, pairs.pair_refs(i))
            for i, count in zip(key_order, counts)
            if count
        ),
    )

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


def aragorn_omnicorp(response_id: str, log_level: int = logging.INFO) -> list[dict]:
    """Process-pool entrypoint: load, overlay, and save entirely in the child.

    Only the small ``response_id`` string crosses the process-pool boundary; the
    (potentially very large) message is read from Redis, mutated, and written
    back inside the child process. Previously the whole message was pickled into
    the child and the mutated copy pickled back out, so a single big query was
    resident several times over -- across the parent event loop, the pickle
    buffers, and the child -- which is what pushed this worker into OOM. Reading
    and writing here keeps the payload off the parent's heap entirely.

    The task's logger can't cross the boundary either (it isn't picklable), so
    the child logs at the task's ``log_level`` through its own logger and
    returns the formatted records, oldest-first, for the parent to fold into
    the query's logs.
    """
    with pool_task_logger(STREAM, log_level) as (logger, query_log_handler):
        # The cyclic GC is off for the task: a big message is millions of
        # dicts and lists, and every collection pass rescans them -- that was
        # about 40% of a large overlay's time. The decoded message and
        # everything the overlay builds are acyclic, so reference counting
        # frees them all without it.
        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            message = get_message_sync(response_id)

            workflow = None
            if "workflow" in message:
                workflow = message["workflow"]
                del message["workflow"]

            response = omnicorp_overlay(message, logger)

            if workflow is not None:
                response["workflow"] = workflow

            save_response_sync(response_id, response)
        finally:
            if gc_was_enabled:
                gc.enable()
        return query_log_handler.drain()


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
        entries = await pool.run_within_budget(
            loop,
            task[1],
            OVERLAY_STEP,
            aragorn_omnicorp,
            response_id,
            logger.getEffectiveLevel(),
        )
        ingest_query_logs(logger, entries)

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
