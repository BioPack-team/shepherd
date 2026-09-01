"""Path scoring module"""

import asyncio
import logging
import os
import time
import uuid

import lmdb
import numpy as np
import torch
from bmt import Toolkit
from torch import nn

from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.data_download import ensure_pathfinder_embeddings
from shepherd_utils.db import get_message_sync, save_message_sync
from shepherd_utils.logger import QueryLogger, get_query_handler, get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import get_tasks, run_task_lifecycle

STREAM = "score_paths"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 4
EMBEDDING_DIR = settings.pathfinder_embeddings_dir
MODEL_WEIGHTS = "model_weights/squashbert_direct_3hop.pt"
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

# Per-child scoring state, built on first use by ``_ensure_scoring_state``.
# These live in the process-pool children, not the parent: the parent only
# validates the data at startup and never scores anything itself.
bmt = None
embedding_env = None
mlp = None


def convert_path_to_components(source, target, path, knowledge_graph, logger):
    try:
        edges = knowledge_graph["edges"]
        nodes = knowledge_graph["nodes"]
        ordered = [source]
        while target not in ordered:
            noncylcic = False
            for eid in path:
                edge = edges.get(eid)
                if not edge:
                    continue
                tail = ordered[-1]
                if edge["subject"] == tail and edge["object"] not in ordered:
                    ordered.append(edge["object"])
                    noncylcic = True
                elif edge["object"] == tail and edge["subject"] not in ordered:
                    ordered.append(edge["subject"])
                    noncylcic = True
            if not noncylcic:
                return None  # Cyclic paths are not supported
        if len(ordered) != 4:
            return None
        names = []
        cat_phrases = []
        for nid in ordered:
            node = nodes.get(nid)
            if node is None:
                return None
            name = node.get("name")
            cats = node.get("categories") or []
            if not name or not cats or not cats[0]:
                return None
            names.append(name)
            cat_phrases.append(cats[0].removeprefix("biolink:"))
        hop_phrases = []
        for cur, nxt in zip(ordered[:-1], ordered[1:]):
            preds = []
            seen = set()
            for eid in path:
                edge = edges.get(eid)
                if not edge:
                    continue
                pname = edge["predicate"].removeprefix("biolink:").replace("_", " ")
                if not bmt.is_predicate(pname):
                    continue  # mirror cache: drop non-biolink predicates
                if edge["subject"] == cur and edge["object"] == nxt:
                    name = pname
                elif edge["object"] == cur and edge["subject"] == nxt:
                    if bmt.is_symmetric(pname):
                        name = pname
                    else:
                        inv = bmt.get_inverse(pname)
                        if inv is None:
                            continue  # mirror cache: silently drop non-invertibles
                        name = inv
                else:
                    continue
                if name not in seen:
                    seen.add(name)
                    preds.append(name)
            if not preds:
                return None
            preds.sort()
            hop_phrases.append(
                preds[0] if len(preds) == 1 else "[" + " or ".join(preds) + "]"
            )
        return names, cat_phrases, hop_phrases
    except Exception as e:
        logger.error(f"Failed to convert path to components: {e}")
        return None


def _lookup(txn, key):
    raw = txn.get(key.encode("utf-8"))
    if raw is None:
        raise KeyError(key)
    return np.frombuffer(raw, dtype=np.float16)


def _probe_cache(env):
    """Confirm the LMDB cache is non-empty and decodes to (768,) float16."""
    with env.begin() as txn:
        n = txn.stat()["entries"]
        if n == 0:
            raise RuntimeError("embeddings cache is empty")
        cursor = txn.cursor()
        cursor.first()
        key, value = cursor.item()
        expected_bytes = 768 * np.dtype(np.float16).itemsize
        if len(value) != expected_bytes:
            raise RuntimeError(
                f"embeddings cache has wrong value size: got {len(value)} bytes, "
                f"expected {expected_bytes} (768-dim float16)"
            )
        return n, key.decode("utf-8", errors="replace")


def _open_embeddings():
    """Open the embeddings LMDB read-only.

    ``lock=False`` on a read-only env is what lets every pool child map the same
    database concurrently; the pages are shared through the OS page cache rather
    than copied per child.
    """
    return lmdb.open(
        EMBEDDING_DIR, readonly=True, lock=False, readahead=False, subdir=True
    )


def _build_mlp(logger):
    """Build the scoring MLP and load its trained weights.

    The weights are memory-mapped and assigned rather than copied, so the pool's
    children share one 61 MB mapping instead of each allocating its own copy.
    ``mmap=True`` hands back file-backed ``MAP_PRIVATE`` tensors; ``assign=True``
    makes those tensors *be* the module's parameters instead of a destination to
    copy into (the default would allocate fresh storage per child and undo the
    sharing). Scoring only ever reads them -- the model is in ``eval`` mode under
    ``inference_mode`` -- so nothing triggers a copy-on-write fault and the pages
    stay shared for the life of the pod.

    ``mmap=True`` needs a checkpoint in torch's zipfile format (the default since
    torch 1.6). A checkpoint re-saved in the legacy format would raise, so fall
    back to a plain load: that child then pays for its own copy, which is the
    pre-mmap behaviour and strictly better than failing every task.
    """
    model = nn.Sequential(
        nn.Linear(11 * 768, 1536),
        nn.GELU(),
        nn.LayerNorm(1536),
        nn.Linear(1536, 1536),
        nn.GELU(),
        nn.LayerNorm(1536),
        nn.Linear(1536, 1),
    )
    try:
        ckpt = torch.load(MODEL_WEIGHTS, map_location="cpu", mmap=True)
        assign = True
    except (RuntimeError, ValueError) as e:
        logger.warning(
            f"Could not memory-map {MODEL_WEIGHTS} ({e}); loading a private copy "
            "of the weights instead. Every pool child will hold its own."
        )
        ckpt = torch.load(MODEL_WEIGHTS, map_location="cpu")
        assign = False
    model.load_state_dict(
        {k.removeprefix("net."): v for k, v in ckpt["model"].items()}, assign=assign
    )
    model.eval()
    return model


def _validate_scoring_data(logger) -> None:
    """Fail fast at startup if the data the pool children need is missing.

    The children do the actual loading, so without this an empty volume mount or
    a missing checkpoint would surface only as every task failing individually.
    The env opened here is closed again immediately -- the parent never scores.
    """
    env = _open_embeddings()
    try:
        count, sample = _probe_cache(env)
    finally:
        env.close()
    logger.info(f"embeddings cache: {count} entries (sample key: {sample!r})")
    if not os.path.exists(MODEL_WEIGHTS):
        raise RuntimeError(f"model weights not found at {MODEL_WEIGHTS}")


def _ensure_scoring_state(logger) -> None:
    """Build this child's scoring state on first use, then reuse it.

    Loaded lazily rather than through the pool's ``initializer`` so a failure
    here (an unreadable LMDB, a corrupt checkpoint) surfaces as an ordinary task
    failure with a traceback, instead of killing the child before it takes any
    work and leaving the pool to rebuild itself in a loop. Each child pays this
    once and amortizes it over ``pool_max_tasks_per_child`` tasks.

    Two of the three are shared across children rather than duplicated: the
    embeddings LMDB and the model weights are both file-backed mappings, so the
    OS page cache serves every child from one copy (see ``_open_embeddings`` and
    ``_build_mlp``). The biolink ``Toolkit`` is live Python objects and so is
    genuinely per-child -- the one place pool size costs real memory, which is
    why ``POOL_MAX_WORKERS`` exists for a memory-tight deployment.
    """
    global bmt, embedding_env, mlp
    if mlp is not None:
        return
    # One intra-op thread per child. The pool is already sized to the pod's CPU
    # allocation, so letting each child spin up a full torch thread pool
    # oversubscribes that quota several times over and the children mostly end
    # up contending with each other.
    torch.set_num_threads(1)
    bmt = Toolkit()
    embedding_env = _open_embeddings()
    mlp = _build_mlp(logger)
    logger.debug(f"score_paths child {os.getpid()} loaded its scoring state.")


def score_paths(response_id, logger):
    message = get_message_sync(response_id)
    try:
        paths = message["message"]["query_graph"]["paths"]
        results = message["message"]["results"]
        knowledge_graph = message["message"]["knowledge_graph"]
        auxiliary_graphs = message["message"].get("auxiliary_graphs") or {}
        qpath_id, qpath = next(iter(paths.items()))
        subject_qnode = qpath["subject"]
        object_qnode = qpath["object"]
        total_analyses = sum(len(r.get("analyses", [])) for r in results)
        logger.info(
            f"Scoring {response_id}: {len(results)} results, "
            f"{total_analyses} analyses, {len(auxiliary_graphs)} aux graphs"
        )
        feature_rows = []
        embedding_index = []
        skip_no_binding = 0
        skip_bad_path = 0
        skip_missing_emb = 0
        missing_samples = []
        t0 = time.time()
        with embedding_env.begin() as txn:
            for result_ind, result in enumerate(results):
                try:
                    source = result["node_bindings"][subject_qnode][0]["id"]
                    target = result["node_bindings"][object_qnode][0]["id"]
                except (KeyError, IndexError, TypeError):
                    continue
                analyses = result.get("analyses", [])
                for analysis_ind, analysis in enumerate(analyses):
                    path_bindings = analysis.get("path_bindings", {}).get(qpath_id, [])
                    try:
                        aux_id = path_bindings[0]["id"]
                        edge_ids = auxiliary_graphs[aux_id]["edges"]
                    except (KeyError, IndexError, TypeError):
                        analysis["score"] = 0.0
                        skip_no_binding += 1
                        continue
                    components = convert_path_to_components(
                        source, target, edge_ids, knowledge_graph, logger
                    )
                    if components is None:
                        analysis["score"] = 0.0
                        skip_bad_path += 1
                        continue
                    names, cats, hops = components
                    try:
                        features = np.concatenate(
                            [
                                _lookup(txn, names[0]),
                                _lookup(txn, cats[0]),
                                _lookup(txn, hops[0]),
                                _lookup(txn, names[1]),
                                _lookup(txn, cats[1]),
                                _lookup(txn, hops[1]),
                                _lookup(txn, names[2]),
                                _lookup(txn, cats[2]),
                                _lookup(txn, hops[2]),
                                _lookup(txn, names[3]),
                                _lookup(txn, cats[3]),
                            ]
                        )
                    except KeyError as e:
                        key = e.args[0]
                        if len(missing_samples) < 5 and key not in missing_samples:
                            missing_samples.append(key)
                        analysis["score"] = 0.0
                        skip_missing_emb += 1
                        continue
                    feature_rows.append(features)
                    embedding_index.append((result_ind, analysis_ind))
        build_time = time.time() - t0
        skipped = skip_no_binding + skip_bad_path + skip_missing_emb
        msg = f"Feature build: {len(feature_rows)}/{total_analyses} ready in {build_time:.1f}s"
        if skipped:
            msg += (
                f"; skipped {skipped} "
                f"(no binding: {skip_no_binding}, "
                f"bad path: {skip_bad_path}, "
                f"missing embedding: {skip_missing_emb})"
            )
            if missing_samples:
                msg += f"; missing keys e.g. {missing_samples}"
        logger.info(msg)
        if feature_rows:
            features = np.stack(feature_rows).astype(np.float32)
            t0 = time.time()
            with torch.inference_mode():
                logits = mlp(torch.from_numpy(features)).squeeze(-1)
                all_scores = torch.sigmoid(logits).numpy()
            mlp_time = time.time() - t0

            scores = []
            for (r_idx, a_idx), s in zip(embedding_index, all_scores):
                s = float(s)
                results[r_idx]["analyses"][a_idx]["score"] = s
                scores.append(s)

            logger.info(
                f"Scored {len(scores)} paths in {mlp_time:.1f}s; "
                f"scores [{min(scores):.3f}, {max(scores):.3f}] "
                f"mean {sum(scores) / len(scores):.3f}"
            )
        else:
            logger.info("No paths to score")
    except Exception as e:
        logger.error(f"Error scoring paths: {e}", exc_info=True)
        for result in message["message"].get("results", []) or []:
            for analysis in result.get("analyses", []) or []:
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
    # own copy of the MLP plus a full message, so this also bounds peak memory.
    # POOL_MAX_WORKERS overrides.
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
