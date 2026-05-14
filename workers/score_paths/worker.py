"""Path scoring module"""
import asyncio
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import lmdb
import numpy as np
import torch
from bmt import Toolkit
from torch import nn
from xgboost import XGBClassifier

from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, handle_task_failure, wrap_up_task

STREAM = "score_paths"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 1
EMBEDDING_DIR = "pathfinder_embeddings"
tracer = setup_tracer(STREAM)


def convert_path_to_components(source, target, path, knowledge_graph, logger):
    try:
        edges = knowledge_graph["edges"]
        nodes = knowledge_graph["nodes"]
        ordered = [source]
        while target not in ordered:
            progress = False
            for eid in path:
                edge = edges.get(eid)
                if not edge:
                    continue
                tail = ordered[-1]
                if edge["subject"] == tail and edge["object"] not in ordered:
                    ordered.append(edge["object"])
                    progress = True
                elif edge["object"] == tail and edge["subject"] not in ordered:
                    ordered.append(edge["subject"])
                    progress = True
            if not progress:
                return None
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


async def score_paths(task, logger):
    response_id = task[1]["response_id"]
    message = await get_message(response_id, logger)
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
                        features = np.concatenate([
                            _lookup(txn, names[0]), _lookup(txn, cats[0]),
                            _lookup(txn, hops[0]), _lookup(txn, names[1]), _lookup(txn, cats[1]),
                            _lookup(txn, hops[1]), _lookup(txn, names[2]), _lookup(txn, cats[2]),
                            _lookup(txn, hops[2]), _lookup(txn, names[3]), _lookup(txn, cats[3]),
                        ])
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
            loop = asyncio.get_event_loop()
            mlp_out = await loop.run_in_executor(
                executor, partial(mlp, torch.from_numpy(features))
            )
            mlp_time = time.time() - t0
            path_embeddings = nn.functional.normalize(mlp_out, p=2, dim=1).detach().numpy()
            t0 = time.time()
            scores = []
            for (result_ind, analysis_ind), embedding in zip(embedding_index, path_embeddings):
                try:
                    score = clf.predict_proba(embedding.reshape(1, -1))[:, 1][0]
                except Exception as e:
                    logger.error(f"Failed to score path: {e}")
                    score = 0.0
                results[result_ind]["analyses"][analysis_ind]["score"] = float(score)
                scores.append(float(score))
            clf_time = time.time() - t0
            logger.info(
                f"Scored {len(scores)} paths in {mlp_time + clf_time:.1f}s "
                f"(MLP {mlp_time:.2f}s, classifier {clf_time:.2f}s); "
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
    await save_message(response_id, message, logger)


async def process_task(task, parent_ctx, logger, limiter):
    start = time.time()
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await score_paths(task, logger)
        try:
            await wrap_up_task(STREAM, GROUP, task, logger)
        except Exception as e:
            logger.error(f"Failed to wrap up task: {e}")
    except asyncio.CancelledError:
        logger.warning(f"Task cancelled: {task[0]}")
    except Exception as e:
        logger.error(f"Task {task[0]} failed: {e}", exc_info=True)
        await handle_task_failure(STREAM, GROUP, task, logger)
    finally:
        span.end()
        limiter.release()
        logger.info(f"Task took {time.time() - start} seconds")


async def poll_for_tasks():
    global clf, bmt, mlp, embedding_env, executor
    clf = XGBClassifier()
    clf.load_model("model_weights/squashbert_classifier_weights.json")
    bmt = Toolkit()
    embedding_env = lmdb.open(EMBEDDING_DIR,
                              readonly=True, lock=False, readahead=False, subdir=True)
    count, sample = _probe_cache(embedding_env)
    logging.info(f"embeddings cache: {count} entries (sample key: {sample!r})")
    mlp = nn.Sequential(
        nn.Linear(11 * 768, 1536),
        nn.GELU(),
        nn.LayerNorm(1536),
        nn.Linear(1536, 1536),
        nn.GELU(),
        nn.LayerNorm(1536),
        nn.Linear(1536, 768),
    )
    ckpt = torch.load("model_weights/squashbert_mlp_hop3.pt", map_location="cpu")
    mlp.load_state_dict({k.removeprefix("net."): v for k, v in ckpt["model"].items()})
    mlp.eval()
    executor = ThreadPoolExecutor(max_workers=TASK_LIMIT)
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
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
