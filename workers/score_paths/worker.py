"""Example ARA module."""

import asyncio
import json
import logging
import time
import uuid
from xgboost import XGBClassifier
from sentence_transformers import SentenceTransformer
from bmt import Toolkit
from shepherd_utils.db import get_message, save_message
from shepherd_utils.shared import get_tasks, wrap_up_task
from shepherd_utils.otel import setup_tracer


# Queue name
STREAM = "score_paths"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)
clf = XGBClassifier()
bmt = Toolkit()
model = SentenceTransformer("cambridgeltl/SapBERT-from-PubMedBERT-fulltext")

def get_most_specific_category(categories, logger):
    valid = []
    for cat in categories:
        element = bmt.get_element(cat)
        if not element:
            logger.error(f"Category {cat} doesn't exist.")
            continue
        valid.append(cat)

    if not valid:
        return None

    most_specific = []
    for cat in valid:
        dominated = any(
            cat in bmt.get_ancestors(other, reflexive=False)
            for other in valid if other != cat
        )
        if not dominated:
            most_specific.append(cat)

    return bmt.get_element(most_specific[0])

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

    source_cat = get_most_specific_category(
        knowledge_graph["nodes"][source]["categories"], logger
    )
    if not source_cat:
        raise ValueError(f"Could not determine category for source node {source}.")

    path_sentence = f"{knowledge_graph['nodes'][source]['name']} (a {source_cat.name}) "
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
        node_cat = get_most_specific_category(
            knowledge_graph["nodes"][path_node]["categories"], logger
        )
        if not node_cat:
            raise ValueError(f"Could not determine category for node {path_node}.")
        path_sentence += (
            f" {knowledge_graph['nodes'][path_node]['name']} (a {node_cat.name})"
        )

    logger.debug(f"Generated sentence: {path_sentence}")
    return path_sentence

async def score_paths(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(response_id, logger)
    current_op = workflow[0]

    try:
        for qpath_id, qpath in message["message"]["query_graph"]["paths"].items():
            source_qnode = qpath["subject"]
            target_qnode = qpath["object"]

            for ind, result in enumerate(message["message"]["results"]):
                # Fatal at the result level: if we can't find source/target
                # node bindings the result is malformed, skip it
                try:
                    source = result["node_bindings"][source_qnode][0]["id"]
                    target = result["node_bindings"][target_qnode][0]["id"]
                except KeyError as e:
                    logger.error(
                        f"Result {ind} missing expected node binding {e}, skipping."
                    )
                    continue

                # Pass 1: collect all sentences for this result
                tasks = []
                for ana_ind, analysis in enumerate(result["analyses"]):
                    try:
                        path_id = analysis["path_bindings"][qpath_id][0]["id"]
                        sentence = convert_path_to_sentence(
                            source,
                            target,
                            message["message"]["auxiliary_graphs"][path_id]["edges"],
                            message["message"]["knowledge_graph"],
                            logger,
                        )
                        tasks.append((ana_ind, sentence))
                    except KeyError as e:
                        logger.error(
                            f"Result {ind}, analysis {ana_ind}: missing key {e}, skipping analysis."
                        )
                        continue
                    except ValueError as e:
                        logger.error(
                            f"Result {ind}, analysis {ana_ind}: could not build sentence."
                        )
                        continue

                if not tasks:
                    logger.warning(
                        f"Result {ind}: no valid analyses to score, skipping."
                    )
                    continue

                all_sentences = [t[1] for t in tasks]
                try:
                    all_embeddings = model.encode(
                        all_sentences, batch_size=32, show_progress_bar=False
                    )
                except Exception as e:
                    logger.error(
                        f"Result {ind}: embedding failed due to {e}."
                    )
                    message["message"]["results"][ind]["analyses"][ana_ind][
                        "score"
                    ] = 0.0
                    continue

                for (ana_ind, _), embedding in zip(tasks, all_embeddings):
                    try:
                        probs = clf.predict_proba(embedding.reshape(1, -1))[:, 1]
                        message["message"]["results"][ind]["analyses"][ana_ind][
                            "score"
                        ] = float(probs[0])
                    except Exception as e:
                        logger.error(
                            f"Result {ind}, analysis {ana_ind}: scoring failed due to {e}."
                        )
                        message["message"]["results"][ind]["analyses"][ana_ind][
                            "score"
                        ] = 0.0
                        continue
    except KeyError as e:
        # can't find the right structure of message
        err = f"Error scoring paths: {e}"
        logger.error(err)
        raise KeyError(err)
    logger.info("Returning scored paths.")

    # save merged message back to db
    await save_message(response_id, message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await score_paths(task, logger)
    finally:
        span.end()
        limiter.release()


async def poll_for_tasks():
    async for task, parent_ctx, logger, limiter in get_tasks(
        STREAM, GROUP, CONSUMER, TASK_LIMIT
    ):
        asyncio.create_task(process_task(task, parent_ctx, logger, limiter))


if __name__ == "__main__":
    clf.load_model("workers/score_paths/model_weights/sapbert_classifier_weights.json")
    asyncio.run(poll_for_tasks())
