"""Path scoring module: every analysis scored by the sequence reranker
(``scorer``), from the LMDB squashbert's scripts/export_reranker.py writes."""

import asyncio
import logging
import os
import time
import uuid

import lmdb
import torch

from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.data_download import ensure_pathfinder_embeddings
from shepherd_utils.db import get_message_sync, save_message_sync
from shepherd_utils.logger import QueryLogger, get_query_handler, get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import get_tasks, run_task_lifecycle

import scorer

STREAM = "score_paths"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 6
EMBEDDING_DIR = settings.pathfinder_embeddings_dir
# Paths scored per forward pass, per length. It bounds peak memory (about 40 KB
# of inputs a path) whatever the message's size: scoring a whole message in
# one pass is what OOM-killed the pod on large messages.
SCORE_CHUNK_SIZE = 1024
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

# Per-child scoring state, built on first use by ``_ensure_scoring_state``.
# These live in the process-pool children, not the parent: the parent only
# validates the data at startup and never scores anything itself.
bmt = None
embedding_env = None
model = None
config = None


def _open_embeddings():
    """Open the embeddings LMDB read-only.

    ``lock=False`` on a read-only env is what lets every pool child map the same
    database concurrently; the pages are shared through the OS page cache rather
    than copied per child.
    """
    return lmdb.open(
        EMBEDDING_DIR, readonly=True, lock=False, readahead=False, subdir=True
    )


def _validate_scoring_data(logger) -> None:
    """Fail fast at startup if the data the pool children need is missing.

    The children do the actual loading, so without this an empty volume mount or
    an LMDB without a reranker would surface only as every task failing
    individually. The env opened here is closed again immediately -- the parent
    never scores.
    """
    env = _open_embeddings()
    try:
        with env.begin() as txn:
            entries = txn.stat()["entries"]
            _, found = scorer.load_reranker(txn)
    finally:
        env.close()
    logger.info(
        f"embeddings LMDB: {entries} entries; reranker scores "
        f"{sorted(found['length_scale'])}-hop paths, biolink "
        f"{found['biolink_version']}, node context "
        f"{found['node_context']}, edge context {found['edge_context']}"
    )


def _ensure_scoring_state(logger) -> None:
    """Build this child's scoring state on first use, then reuse it.

    Loaded lazily rather than through the pool's ``initializer`` so a failure
    here (an unreadable LMDB, a corrupt model) surfaces as an ordinary task
    failure with a traceback, instead of killing the child before it takes any
    work and leaving the pool to rebuild itself in a loop. Each child pays this
    once and amortizes it over ``pool_max_tasks_per_child`` tasks.

    The embeddings LMDB is a file-backed mapping, so the OS page cache serves
    every child from one copy (see ``_open_embeddings``). The reranker (about
    16 MB) and the biolink ``Toolkit`` are per-child -- the one place pool size
    costs real memory, which is why ``POOL_MAX_WORKERS`` exists for a
    memory-tight deployment.
    """
    global bmt, embedding_env, model, config
    if model is not None:
        return
    # One intra-op thread per child. The pool is already sized to the pod's CPU
    # allocation, so letting each child spin up a full torch thread pool
    # oversubscribes that quota several times over and the children mostly end
    # up contending with each other.
    torch.set_num_threads(1)
    embedding_env = _open_embeddings()
    with embedding_env.begin() as txn:
        model, config = scorer.load_reranker(txn)
    bmt = scorer.load_toolkit(config)
    logger.debug(f"score_paths child {os.getpid()} loaded its scoring state.")


def score_paths(response_id, logger):
    message = get_message_sync(response_id)
    try:
        results = message["message"].get("results") or []
        auxiliary_graphs = message["message"].get("auxiliary_graphs") or {}
        total_analyses = sum(len(r.get("analyses") or []) for r in results)
        logger.info(
            f"Scoring {response_id}: {len(results)} results, "
            f"{total_analyses} analyses, {len(auxiliary_graphs)} aux graphs"
        )
        started = time.time()
        with embedding_env.begin() as txn:
            stats = scorer.score_message(
                message, txn, model, config, bmt, SCORE_CHUNK_SIZE
            )
        elapsed = time.time() - started
        skipped = (
            stats["no_binding"]
            + stats["bad_path"]
            + stats["no_scale"]
            + stats["missing"]
        )
        msg = (
            f"Scored {stats['scored']}/{stats['analyses']} analyses in "
            f"{elapsed:.1f}s ({stats['model_seconds']:.1f}s in the reranker); "
            f"{stats['direct']} direct"
        )
        if skipped:
            msg += (
                f"; skipped {skipped} "
                f"(no binding: {stats['no_binding']}, "
                f"bad path: {stats['bad_path']}, "
                f"no length scale: {stats['no_scale']}, "
                f"missing embedding: {stats['missing']})"
            )
            if stats["missing_samples"]:
                msg += f"; missing keys e.g. {stats['missing_samples']}"
        logger.info(msg)
    except Exception as e:
        logger.error(f"Error scoring paths: {e}", exc_info=True)
        for result in message["message"].get("results", []) or []:
            for analysis in result.get("analyses") or []:
                analysis.setdefault("score", 0.0)
    for attempt in range(5):
        try:
            save_message_sync(response_id, message)
            break
        except Exception as e:
            if attempt < 4:
                logger.warning(
                    f"Failed to save message {attempt + 1} times. Trying again..."
                )
                time.sleep(0.5)
            else:
                logger.error(f"Failed to save a message into redis: {e}")


def score_paths_task(response_id: str, log_level: int = logging.INFO) -> list[dict]:
    """Process-pool entrypoint: load, score, and save entirely in the child.

    Only the small ``response_id`` and the task's log level cross the process
    boundary; the (potentially very large) message is read from Redis, scored,
    and written back inside the child. That keeps the payload off the parent's
    heap and -- more importantly -- keeps the feature build off the parent's
    event loop. It used to run in a ``ThreadPoolExecutor``, where the path walk
    is mostly pure Python and so holds the GIL: a few concurrent scorings could
    starve the heartbeat past ``HEARTBEAT_TTL_SEC`` and get a live worker's
    tasks reclaimed out from under it (matching the fix already applied to
    arax_pathfinder / aragorn_score / arax_rank).

    Returns this child's log records, already formatted and oldest-first, for
    the parent to fold into the query's logs -- the child can't reach the
    parent's query log handler itself.
    """
    # logging.getLogger hands back the same object for the whole life of the
    # child, so attach a call-scoped handler and remove it in finally --
    # otherwise handlers accumulate across the child's successive tasks and one
    # query's logs leak into the next.
    query_log_handler = QueryLogger().log_handler
    logger = get_worker_logger(f"{STREAM}.worker.{os.getpid()}")
    logger.setLevel(log_level)
    logger.addHandler(query_log_handler)
    try:
        _ensure_scoring_state(logger)
        score_paths(response_id, logger)
        return query_log_handler.drain()
    finally:
        logger.removeHandler(query_log_handler)


async def process_task(task, parent_ctx, logger, limiter, loop, pool):
    """Process a given task and ACK in redis.

    Scoring is CPU-bound, so it is dispatched to a process pool while the span,
    wrap-up, and error handling stay shared with every other worker. The child's
    log records come back with the result and are folded into this task's query
    logger so they still reach the query's log list.
    """

    async def _run(task, logger):
        response_id = task[1]["response_id"]
        entries = await pool.run(
            loop, score_paths_task, response_id, logger.getEffectiveLevel()
        )
        handler = get_query_handler(logger)
        if handler is not None and entries:
            handler.ingest(entries)

    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, _run)


async def poll_for_tasks():
    loop = asyncio.get_running_loop()
    # Ensure the embeddings LMDB exists before any child opens it (a first-run
    # local `docker compose up` starts with the volume-mounted directory empty).
    # No-op once present or when no download URL is configured (e.g. production,
    # where the data is mounted out of band). Downloading in the parent also
    # keeps the children from racing each other for the same archive.
    ensure_pathfinder_embeddings(LOGGER)
    _validate_scoring_data(LOGGER)
    # Size the pool by the pod's actual CPU allocation (cgroup limit), not
    # os.cpu_count() -- see aragorn_omnicorp.poll_for_tasks. Each child holds its
    # own copy of the reranker plus a full message, so this also bounds peak
    # memory. POOL_MAX_WORKERS overrides.
    max_workers = resolve_pool_workers(TASK_LIMIT, LOGGER)
    LOGGER.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="score_paths process pool",
        task_timeout=settings.score_paths_task_timeout_sec,
    )
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, max_workers
            ):
                asyncio.create_task(
                    process_task(task, parent_ctx, logger, limiter, loop, pool)
                )
        except asyncio.CancelledError:
            LOGGER.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
