"""Path scoring module"""

import asyncio
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import torch
from bmt import Toolkit
from sentence_transformers import SentenceTransformer
from xgboost import XGBClassifier

from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, handle_task_failure, wrap_up_task

# Queue name
STREAM = "score_paths"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 1
# Upper bound on the number of unique path sentences we will send to the
# embedding model per task. After dedup we almost never approach this, but it
# keeps worst-case GPU time bounded; analyses beyond the cap get score=0 and
# will be dropped by the downstream filter_analyses_top_n worker.
MAX_SENTENCES_PER_TASK = 10_000
tracer = setup_tracer(STREAM)


def convert_path_to_sentence(source, target, path, knowledge_graph, logger):

    path_node_list = [source]
    while target not in path_node_list:
        progress = False
        for edge_id in path:
            edge = knowledge_graph["edges"].get(edge_id)
            if not edge:
                logger.error(f"Edge {edge_id} not found in knowledge graph.")
                continue
            if edge["subject"] == path_node_list[-1]:
                if edge["object"] not in path_node_list:
                    path_node_list.append(edge["object"])
                    progress = True
            elif edge["object"] == path_node_list[-1]:
                if edge["subject"] not in path_node_list:
                    path_node_list.append(edge["subject"])
                    progress = True
        if not progress:
            logger.error("Disconnected Path")
            raise ValueError(f"Could not construct path from {source} to {target}")

    current_node = source
    path_predicate_list = []
    for hop_num, next_node in enumerate(path_node_list[1:]):
        path_predicate_list.append(set())
        for edge_id in path:
            edge = knowledge_graph["edges"].get(edge_id)
            if not edge:
                logger.error(f"Edge {edge_id} not found in knowledge graph.")
                continue
            pred = edge["predicate"]
            pred_info = bmt.get_element(pred)
            if not pred_info:
                logger.error(f"Predicate {pred} doesn't exist in Biolink model.")
                continue
            pred = pred_info.name
            if edge["subject"] == current_node and edge["object"] == next_node:
                path_predicate_list[hop_num].add(pred)
            elif edge["object"] == current_node and edge["subject"] == next_node:
                if bmt.is_symmetric(pred):
                    path_predicate_list[hop_num].add(pred)
                else:
                    inv = bmt.get_inverse_predicate(pred)
                    if not inv:
                        logger.error(f"No inverse found for predicate {pred}.")
                    else:
                        path_predicate_list[hop_num].add(inv)
        current_node = next_node

    source_cat = knowledge_graph["nodes"][source]["categories"][0]
    if not source_cat:
        raise ValueError(f"Could not determine category for source node {source}.")
    path_sentence = f"{knowledge_graph['nodes'][source]['name']} (a {source_cat.removeprefix('biolink:')}) "
    first_hop = True
    for path_node, hop_predicates in zip(path_node_list[1:], path_predicate_list):
        hop_preds = list(hop_predicates)
        if not first_hop:
            path_sentence += ", which "
        else:
            first_hop = False
        if len(hop_preds) == 0:
            raise ValueError(f"No predicates found for hop to node {path_node}.")
        elif len(hop_preds) == 1:
            path_sentence += f"{hop_preds[0]}"
        else:
            path_sentence += "either ["
            for hop_pred in hop_preds[:-1]:
                path_sentence += f"{hop_pred} or "
            path_sentence += f"{hop_preds[-1]}]"
        node_cat = knowledge_graph["nodes"][path_node]["categories"][0]
        if not node_cat:
            raise ValueError(f"Could not determine category for node {path_node}.")
        path_sentence += f" {knowledge_graph['nodes'][path_node]['name']} (a {node_cat.removeprefix('biolink:')})"

    return path_sentence


async def score_paths(task, logger: logging.Logger):
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    message = await get_message(response_id, logger)

    try:
        results = message["message"].get("results", [])
        knowledge_graph = message["message"]["knowledge_graph"]
        auxiliary_graphs = message["message"]["auxiliary_graphs"]

        for qpath_id, qpath in message["message"]["query_graph"]["paths"].items():
            source_qnode = qpath["subject"]
            target_qnode = qpath["object"]

            # Phase 1 - Gather: collect valid (result_ind, analysis_ind, path_id,
            # source, target) tuples, zeroing analyses whose bindings are missing.
            gathered = []
            for ind, result in enumerate(results):
                try:
                    source = result["node_bindings"][source_qnode][0]["id"]
                    target = result["node_bindings"][target_qnode][0]["id"]
                except KeyError as e:
                    logger.error(
                        f"Result {ind} missing expected node binding {e}, skipping."
                    )
                    continue

                for analysis_ind, analysis in enumerate(result.get("analyses", [])):
                    try:
                        path_id = analysis["path_bindings"][qpath_id][0]["id"]
                    except KeyError as e:
                        logger.error(
                            f"Result {ind}, analysis {analysis_ind}: missing key {e}, skipping analysis."
                        )
                        message["message"]["results"][ind]["analyses"][analysis_ind][
                            "score"
                        ] = 0.0
                        continue
                    gathered.append((ind, analysis_ind, path_id, source, target))

            if not gathered:
                logger.warning(
                    f"qpath {qpath_id}: no valid analyses to score, skipping."
                )
                continue

            # Phase 2a - Build a sentence once per unique (path_id, source, target).
            # Multiple analyses that share the same auxiliary graph collapse here.
            path_sentence_cache = {}
            for _, _, path_id, source, target in gathered:
                key = (path_id, source, target)
                if key in path_sentence_cache:
                    continue
                try:
                    path_sentence_cache[key] = convert_path_to_sentence(
                        source,
                        target,
                        auxiliary_graphs[path_id]["edges"],
                        knowledge_graph,
                        logger,
                    )
                except KeyError as e:
                    logger.error(
                        f"qpath {qpath_id}, path_id {path_id}: missing key {e} while building sentence."
                    )
                    path_sentence_cache[key] = None
                except ValueError:
                    logger.error(
                        f"qpath {qpath_id}, path_id {path_id}: could not build sentence."
                    )
                    path_sentence_cache[key] = None

            # Phase 2b - Dedup by sentence text so each unique sentence is encoded
            # exactly once. records lets us later scatter scores back to every
            # (result_ind, analysis_ind) that consumed a given sentence.
            sentence_to_index = {}
            unique_sentences = []
            records = []  # list of (result_ind, analysis_ind, sentence_idx or None)
            for ind, analysis_ind, path_id, source, target in gathered:
                sentence = path_sentence_cache[(path_id, source, target)]
                if sentence is None:
                    message["message"]["results"][ind]["analyses"][analysis_ind][
                        "score"
                    ] = 0.0
                    records.append((ind, analysis_ind, None))
                    continue
                idx = sentence_to_index.get(sentence)
                if idx is None:
                    idx = len(unique_sentences)
                    sentence_to_index[sentence] = idx
                    unique_sentences.append(sentence)
                records.append((ind, analysis_ind, idx))

            logger.info(
                f"qpath {qpath_id}: gathered={len(gathered)} "
                f"unique_paths={len(path_sentence_cache)} "
                f"unique_sentences={len(unique_sentences)}"
            )

            # Phase 3 - Safety cap: bound worst-case GPU work. Analyses whose
            # sentences fall outside the cap get score=0 and will be dropped by
            # filter_analyses_top_n downstream.
            if len(unique_sentences) > MAX_SENTENCES_PER_TASK:
                logger.warning(
                    f"qpath {qpath_id}: {len(unique_sentences)} unique sentences "
                    f"exceeds cap {MAX_SENTENCES_PER_TASK}; scoring the first "
                    f"{MAX_SENTENCES_PER_TASK} and zeroing the rest."
                )
                unique_sentences = unique_sentences[:MAX_SENTENCES_PER_TASK]
                for ind, analysis_ind, idx in records:
                    if idx is not None and idx >= MAX_SENTENCES_PER_TASK:
                        message["message"]["results"][ind]["analyses"][analysis_ind][
                            "score"
                        ] = 0.0

            if not unique_sentences:
                logger.warning(
                    f"qpath {qpath_id}: no valid sentences after dedup, skipping encode."
                )
                continue

            # Phase 4 - Score: a single model.encode and a single predict_proba
            # call for the whole task, then scatter scores back to records.
            try:
                # Lock prevents concurrent encode() calls corrupting internal
                # state (https://github.com/huggingface/sentence-transformers/issues/794).
                logger.debug("Waiting for embedding model lock.")
                async with embedding_lock:
                    logger.info(
                        f"Generating {len(unique_sentences)} embeddings using "
                        f"device {device} and batch size {embedding_batch_size}."
                    )
                    loop = asyncio.get_running_loop()
                    embeddings = await loop.run_in_executor(
                        executor,
                        partial(
                            model.encode,
                            unique_sentences,
                            batch_size=embedding_batch_size,
                            show_progress_bar=False,
                            convert_to_numpy=True,
                        ),
                    )
                logger.info("Scoring embeddings.")
                scores = clf.predict_proba(embeddings)[:, 1]
            except Exception as e:
                logger.error(f"qpath {qpath_id}: embedding/scoring failed due to {e}.")
                for ind, analysis_ind, idx in records:
                    if idx is not None and idx < len(unique_sentences):
                        message["message"]["results"][ind]["analyses"][analysis_ind][
                            "score"
                        ] = 0.0
                continue

            for ind, analysis_ind, idx in records:
                if idx is None or idx >= len(unique_sentences):
                    # Already zeroed above (missing sentence or beyond cap).
                    continue
                message["message"]["results"][ind]["analyses"][analysis_ind][
                    "score"
                ] = float(scores[idx])
    except KeyError as e:
        # can't find the right structure of message
        err = f"Error scoring paths: {e}"
        logger.error(err)
        raise KeyError(err)
    logger.info("Returning scored paths.")

    # save merged message back to db
    await save_message(response_id, message, logger)

    if torch.cuda.is_available():
        # Torch keeps vram allocated unless we clear cache.
        torch.cuda.empty_cache()


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    start = time.time()
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await score_paths(task, logger)
        try:
            await wrap_up_task(STREAM, GROUP, task, logger)
        except Exception as e:
            logger.error(f"Task {task[0]}: Failed to wrap up task: {e}")
    except asyncio.CancelledError:
        logger.warning(f"Task {task[0]} was cancelled.")
    except Exception as e:
        logger.error(f"Task {task[0]} failed with unhandled error: {e}", exc_info=True)
        await handle_task_failure(STREAM, GROUP, task, logger)
    finally:
        span.end()
        limiter.release()
        logger.info(f"Task took {time.time() - start}")


async def poll_for_tasks():
    global clf, bmt, model, device, embedding_batch_size, executor, embedding_lock
    clf = XGBClassifier()
    clf.load_model("model_weights/sapbert_classifier_weights.json")
    bmt = Toolkit()
    device, embedding_batch_size = (
        ("cuda", 128) if torch.cuda.is_available() else ("cpu", 32)
    )
    model = SentenceTransformer(
        "cambridgeltl/SapBERT-from-PubMedBERT-fulltext", device=device
    )
    model.encode("")
    executor = ThreadPoolExecutor(max_workers=TASK_LIMIT)
    embedding_lock = asyncio.Lock()
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
