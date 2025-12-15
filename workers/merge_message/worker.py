"""Merge two TRAPI messages together."""

import asyncio
import copy
import json
import logging
import os
import time
import uuid
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, Union

from shepherd_utils.broker import acquire_lock, mark_task_as_complete, remove_lock
from shepherd_utils.db import (
    get_message,
    remove_callback_id,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, merge_kgraph

# Queue name
STREAM = "merge_message"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)


def get_edgeset(result):
    """Given a result, return a frozenset of any knowledge edges in it"""
    edgeset = set()
    for analysis in result["analyses"]:
        for edge_id, edgelist in analysis["edge_bindings"].items():
            edgeset.update([e["id"] for e in edgelist])
    return frozenset(edgeset)


def create_aux_graph(analysis):
    """Given an analysis, create an auxiliary graph.
    Look through the analysis edge bindings, get all the knowledge edges, and put them in an aux graph.
    Give it a random uuid as an id."""
    aux_graph_id = str(uuid.uuid4())
    aux_graph = {"edges": [], "attributes": []}
    for edge_id, edgelist in analysis["edge_bindings"].items():
        for edge in edgelist:
            aux_graph["edges"].append(edge["id"])
    return aux_graph_id, aux_graph


def add_knowledge_edge(target, result_message, aux_graph_ids, answer):
    """Create a new knowledge edge in the result message, with the aux graph ids as support."""
    # Find the subject, object, and predicate of the original query
    query_graph = result_message["message"]["query_graph"]
    # get the first key and value from the edges
    qedge_id, qedge = next(iter(query_graph["edges"].items()))
    # For the nodes, if there is an id, then use it in the knowledge edge. If there is not, then use the answer
    qnode_subject_id = qedge["subject"]
    qnode_object_id = qedge["object"]
    if (
        "ids" in query_graph["nodes"][qnode_subject_id]
        and query_graph["nodes"][qnode_subject_id]["ids"] is not None
    ):
        qnode_subject = query_graph["nodes"][qnode_subject_id]["ids"][0]
        qnode_object = answer
    else:
        qnode_subject = answer
        qnode_object = query_graph["nodes"][qnode_object_id]["ids"][0]
    predicate = qedge["predicates"][0]
    if (
        "qualifier_constraints" in qedge
        and qedge["qualifier_constraints"] is not None
        and len(qedge["qualifier_constraints"]) > 0
    ):
        qualifiers = qedge["qualifier_constraints"][0]["qualifier_set"]
    else:
        qualifiers = None
    # Create a new knowledge edge
    new_edge_id = str(uuid.uuid4())
    source = f"infores:shepherd-{target}"
    new_edge = {
        "subject": qnode_subject,
        "object": qnode_object,
        "predicate": predicate,
        "attributes": [
            {"attribute_type_id": "biolink:support_graphs", "value": aux_graph_ids},
            {
                "attribute_type_id": "biolink:agent_type",
                "value": "computational_model",
                "attribute_source": source,
            },
            {
                "attribute_type_id": "biolink:knowledge_level",
                "value": "prediction",
                "attribute_source": source,
            },
        ],
        # Shepherd is the primary ks because shepherd inferred the existence of this edge.
        "sources": [
            {
                "resource_id": source,
                "resource_role": "primary_knowledge_source",
                "upstream_resource_ids": [],
            }
        ],
    }
    if qualifiers is not None:
        new_edge["qualifiers"] = qualifiers
    result_message["message"]["knowledge_graph"]["edges"][new_edge_id] = new_edge
    return new_edge_id


def merge_answer(target, result_message, answer, results, qnode_ids):
    """Given a set of results and the node identifiers of the original qgraph,
    create a single message.
    result_message has to contain the original query graph
    The original qgraph is a creative mode query, which has been expanded into a set of
    rules and run as straight queries using either strider or robokopkg.
    results contains both the lookup results and the creative results, separated out by keys
    Each result coming in is now structured like this:
    result
        node_bindings: Binding to the rule qnodes. includes bindings to original qnode ids
        analysis:
            edge_bindings: Binding to the rule edges.
    To merge the answer, we need to
    0) Filter out any creative results that exactly replicate a lookup result
    1) create node bindings for the original creative qnodes
    2) convert the analysis of each input result into an auxiliary graph
    3) Create a knowledge edge corresponding to the original creative query edge
    4) add the aux graphs as support for this knowledge edge
    5) create an analysis with an edge binding from the original creative query edge to the new knowledge edge
    6) add any lookup edges to the analysis directly
    """
    # 0. Filter out any creative results that exactly replicate a lookup result
    # How does this happen?   Suppose it's an inferred treats.  Lookup will find a direct treats
    # But a rule that ameliorates implies treats will also return a direct treats because treats
    # is a subprop of ameliorates. We assert that the two answers are the same if the set of their
    # kgraph edges are the same.
    # There are also cases where subpredicates in rules can lead to the same answer.  So here we
    # also unify that.   If we decide to pass rules along with the answers, we'll have to be a bit
    # more careful.
    lookup_edgesets = [get_edgeset(result) for result in results["lookup"]]
    creative_edgesets = set()
    creative_results = []
    for result in results["creative"]:
        creative_edges = get_edgeset(result)
        if creative_edges in lookup_edgesets:
            continue
        elif creative_edges in creative_edgesets:
            continue
        else:
            creative_edgesets.add(creative_edges)
            creative_results.append(result)
    results["creative"] = creative_results
    # 1. Create node bindings for the original creative qnodes and lookup qnodes
    mergedresult = {"node_bindings": {}, "analyses": []}
    serkeys = defaultdict(set)
    for q in qnode_ids:
        mergedresult["node_bindings"][q] = []
        for result in results["creative"] + results["lookup"]:
            for nb in result["node_bindings"][q]:
                serialized_binding = json.dumps(nb, sort_keys=True)
                if serialized_binding not in serkeys[q]:
                    mergedresult["node_bindings"][q].append(nb)
                    serkeys[q].add(serialized_binding)

    # 2. convert the analysis of each input result into an auxiliary graph
    aux_graph_ids = []
    if (
        "auxiliary_graphs" not in result_message["message"]
        or result_message["message"]["auxiliary_graphs"] is None
    ):
        result_message["message"]["auxiliary_graphs"] = {}
    for result in results["creative"]:
        for analysis in result["analyses"]:
            aux_graph_id, aux_graph = create_aux_graph(analysis)
            result_message["message"]["auxiliary_graphs"][aux_graph_id] = aux_graph
            aux_graph_ids.append(aux_graph_id)

    # 3. Create a knowledge edge corresponding to the original creative query edge
    # 4. and add the aux graphs as support for this knowledge edge
    knowledge_edge_ids = []
    if len(aux_graph_ids) > 0:
        # only do this if there are creative results.  There could just be a lookup
        for nid in answer:
            knowledge_edge_id = add_knowledge_edge(
                target, result_message, aux_graph_ids, nid
            )
            knowledge_edge_ids.append(knowledge_edge_id)

    # 5. create an analysis with an edge binding from the original creative query edge to the new knowledge edge
    qedge_id = list(result_message["message"]["query_graph"]["edges"].keys())[0]
    analysis = {
        "resource_id": f"infores:shepherd-{target}",
        "edge_bindings": {
            qedge_id: [{"id": kid, "attributes": []} for kid in knowledge_edge_ids]
        },
    }
    mergedresult["analyses"].append(analysis)

    # 6. add any lookup edges to the analysis directly
    for result in results["lookup"]:
        for analysis in result["analyses"]:
            for qedge in analysis["edge_bindings"]:
                if qedge not in mergedresult["analyses"][0]["edge_bindings"]:
                    mergedresult["analyses"][0]["edge_bindings"][qedge] = []
                mergedresult["analyses"][0]["edge_bindings"][qedge].extend(
                    analysis["edge_bindings"][qedge]
                )

    # result_message["message"]["results"].append(mergedresult)
    return mergedresult


def queries_equivalent(query1, query2):
    """Compare 2 query graphs.  The nuisance is that there is flexiblity in e.g. whether there is a qualifier constraint
    as none or it's not in there or its an empty list.  And similar for is_set and is_set is False.
    """
    q1 = query1.copy()
    q2 = query2.copy()
    for q in [q1, q2]:
        for node in q["nodes"].values():
            if "is_set" in node and node["is_set"] is False:
                del node["is_set"]
            if (
                "set_interpretation" in node and node["set_interpretation"] == "BATCH"
            ) or ("set_interpretation" in node and node["set_interpretation"] is None):
                del node["set_interpretation"]
            if "constraints" in node and len(node["constraints"]) == 0:
                del node["constraints"]
            if "member_ids" in node and len(node["member_ids"]) == 0:
                del node["member_ids"]
            if "ids" in node and node["ids"] is None:
                del node["ids"]
        for edge in q["edges"].values():
            if (
                "attribute_constraints" in edge
                and len(edge["attribute_constraints"]) == 0
            ):
                del edge["attribute_constraints"]
            if (
                "qualifier_constraints" in edge
                and len(edge["qualifier_constraints"]) == 0
            ):
                del edge["qualifier_constraints"]
            if "knowledge_type" in edge:
                del edge["knowledge_type"]
            # handle treats and treats_or_applied_or_studied_to_treat
            for pred_indx, predicate in enumerate(edge["predicates"]):
                if predicate == "biolink:treats":
                    edge["predicates"][
                        pred_indx
                    ] = "biolink:treats_or_applied_or_studied_to_treat"
    return q1 == q2


def group_results_by_qnode(merge_qnode, result_message, lookup_results):
    """merge_qnode is the qnode_id of the node that we want to group by
    result_message is the response message, and its results element  contains all of the creative mode results
    lookup_results is just a results element from the lookup mode query.
    """
    original_results = result_message["message"].get("results", [])
    # group results
    grouped_results = defaultdict(lambda: {"creative": [], "lookup": []})
    # Group results by the merge_qnode
    for result_set, result_key in [
        (original_results, "creative"),
        (lookup_results, "lookup"),
    ]:
        for result in result_set:
            answer = result["node_bindings"][merge_qnode]
            bound = frozenset([x["id"] for x in answer])
            grouped_results[bound][result_key].append(result)
    return grouped_results


def merge_results_by_node(target, result_message, merge_qnode, lookup_results):
    """This assumes a single result message, with a single merged KG.  The goal is to take all results that share a
    binding for merge_qnode and combine them into a single result.
    Assumes that the results are not scored."""
    grouped_results = group_results_by_qnode(
        merge_qnode, result_message, lookup_results
    )
    original_qnodes = result_message["message"]["query_graph"]["nodes"].keys()
    new_results = []
    for r in grouped_results:
        new_result = merge_answer(
            target, result_message, r, grouped_results[r], original_qnodes
        )
        new_results.append(new_result)
    result_message["message"]["results"] = new_results
    return result_message


def get_answer_node(query_graph: Dict[str, Any]) -> Union[str, None]:
    """From the original query graph, get the answer node id."""
    answer_node = None
    qnodes = query_graph.get("nodes", {})
    for qnode_id, qnode in qnodes.items():
        if qnode.get("ids") is None:
            answer_node = qnode_id
    return answer_node


def merge_messages(
    target: str,
    original_query_graph: Dict[str, Any],
    response: Dict[str, Any],
    new_response: Dict[str, Any],
    logger: logging.Logger,
):
    pydantic_kgraph = {"nodes": {}, "edges": {}}
    for result_message in [response, new_response]:
        result_kgraph = (
            result_message["message"]["knowledge_graph"]
            if result_message["message"].get("knowledge_graph") is not None
            else {"nodes": {}, "edges": {}}
        )
        source = f"infores:shepherd-{target}"
        pydantic_kgraph = merge_kgraph(pydantic_kgraph, result_kgraph, source, logger)
    # Construct the final result message, currently empty
    result = {
        "message": {
            "query_graph": {"nodes": {}, "edges": {}},
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "results": [],
            "auxiliary_graphs": {},
        },
        "logs": [],
    }
    result["message"]["query_graph"] = original_query_graph
    result["message"]["knowledge_graph"] = pydantic_kgraph
    for result_message in [response, new_response]:
        if "auxiliary_graphs" in result_message["message"]:
            for aux_id, aux_dict in result_message["message"][
                "auxiliary_graphs"
            ].items():
                if aux_id in result["message"]["auxiliary_graphs"]:
                    for key, val in aux_dict.items():
                        if key in result["message"]["auxiliary_graphs"][aux_id]:
                            if isinstance(
                                result["message"]["auxiliary_graphs"][aux_id][key], list
                            ):
                                # combine both lists and then list/set it for uniqueness
                                result["message"]["auxiliary_graphs"][aux_id][key] = (
                                    list(
                                        set(
                                            result["message"]["auxiliary_graphs"][
                                                aux_id
                                            ][key]
                                            + val
                                        )
                                    )
                                )
                            else:
                                logger.warning(
                                    f"Message had an invalid aux graph property: {key}"
                                )
                        else:
                            result["message"]["auxiliary_graphs"][aux_id][key] = (
                                copy.deepcopy(val)
                            )
                else:
                    result["message"]["auxiliary_graphs"][aux_id] = copy.deepcopy(
                        aux_dict
                    )
    # The result with the direct lookup needs to be handled specially.   It's the one with the lookup query graph
    lookup_results = (
        response["message"]["results"]
        if response["message"].get("results") is not None
        else []
    )
    is_direct_lookup = queries_equivalent(
        new_response["message"]["query_graph"], original_query_graph
    )
    if is_direct_lookup:
        lookup_results.extend(new_response["message"]["results"])
    else:
        result["message"]["results"].extend(
            new_response["message"]["results"]
            if new_response["message"].get("results") is not None
            else []
        )

    answer_node_id = get_answer_node(original_query_graph)
    merged_messages = merge_results_by_node(
        target, result, answer_node_id, lookup_results
    )
    return merged_messages


async def poll_for_tasks():
    loop = asyncio.get_running_loop()
    cpu_count = os.cpu_count()
    cpu_count = cpu_count if cpu_count is not None else 1
    cpu_count = min(cpu_count, TASK_LIMIT)
    executor = ProcessPoolExecutor(max_workers=cpu_count)
    async for task, parent_ctx, logger, limiter in get_tasks(
        STREAM, GROUP, CONSUMER, cpu_count
    ):
        span = tracer.start_span(STREAM, context=parent_ctx)
        query_id = task[1]["query_id"]
        response_id = task[1]["response_id"]
        callback_id = task[1]["callback_id"]
        target = task[1]["target"]
        got_lock = await acquire_lock(response_id, CONSUMER, logger)
        if got_lock:
            logger.info(f"[{callback_id}] Obtained lock.")

            # given a task, get the message from the db
            original_query = await get_message(query_id, logger)
            if original_query is None:
                logger.error(
                    f"Failed to get original query for {query_id}. Discarding callback response."
                )
                await remove_lock(response_id, CONSUMER, logger)
                await remove_callback_id(callback_id, logger)
                limiter.release()
                await mark_task_as_complete(STREAM, GROUP, task[0], logger)
                span.end()
                continue
            original_query_graph = original_query["message"]["query_graph"]
            callback_response = await get_message(callback_id, logger)
            lock_time = time.time()
            original_response = await get_message(response_id, logger)
            # do message merging
            merged_message = await loop.run_in_executor(
                executor,
                merge_messages,
                target,
                original_query_graph,
                original_response,
                callback_response,
                logger,
            )
            # save merged message back to db
            await save_message(response_id, merged_message, logger)
            logger.info(
                f"[{callback_id}] Kept the lock for {time.time() - lock_time} seconds"
            )
            # remove lock so others can now modify message
            await remove_lock(response_id, CONSUMER, logger)
        else:
            logger.error(
                f"Failed to obtain lock for {query_id}. Discarding callback response."
            )
        await remove_callback_id(callback_id, logger)
        limiter.release()
        await mark_task_as_complete(STREAM, GROUP, task[0], logger)
        span.end()


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
