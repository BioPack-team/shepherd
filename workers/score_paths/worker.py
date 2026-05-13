"""Path scoring module"""
import asyncio
import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial

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
                if edge["subject"] == cur and edge["object"] == nxt:
                    name = pname
                elif edge["object"] == cur and edge["subject"] == nxt:
                    if bmt.is_symmetric(pname):
                        name = pname
                    else:
                        inv = bmt.get_inverse(pname)
                        if inv is None:
                            return None
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
        feature_rows = []
        embedding_index = []
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
                    continue
                components = convert_path_to_components(
                    source, target, edge_ids, knowledge_graph, logger
                )
                if components is None:
                    analysis["score"] = 0.0
                    continue
                names, cats, hops = components
                try:
                    features = np.concatenate([
                        embeddings[names[0]], embeddings[cats[0]],
                        embeddings[hops[0]], embeddings[names[1]], embeddings[cats[1]],
                        embeddings[hops[1]], embeddings[names[2]], embeddings[cats[2]],
                        embeddings[hops[2]], embeddings[names[3]], embeddings[cats[3]],
                    ])
                except KeyError as e:
                    logger.error(f"Missing embedding for {e}; scoring 0.0")
                    analysis["score"] = 0.0
                    continue
                feature_rows.append(features)
                embedding_index.append((result_ind, analysis_ind))
        if feature_rows:
            features = np.stack(feature_rows).astype(np.float32)
            loop = asyncio.get_event_loop()
            mlp_out = await loop.run_in_executor(executor, partial(mlp, torch.from_numpy(features)))
            path_embeddings = nn.functional.normalize(mlp_out, p=2, dim=1).detach().numpy()
            for (result_ind, analysis_ind), embedding in zip(embedding_index, path_embeddings):
                try:
                    score = clf.predict_proba(embedding.reshape(1, -1))[:, 1][0]
                except Exception as e:
                    logger.error(f"Failed to score path: {e}")
                    score = 0.0
                results[result_ind]["analyses"][analysis_ind]["score"] = float(score)
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
    global clf, bmt, mlp, embeddings, executor
    clf = XGBClassifier()
    clf.load_model("model_weights/squashbert_classifier_weights.json")
    bmt = Toolkit()
    with open("model_weights/squashbert_embeddings.json") as f:
        embeddings = {k: np.asarray(v, dtype=np.float32) for k, v in json.load(f).items()}
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
