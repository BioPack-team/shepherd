"""Merge two TRAPI messages together."""

import asyncio
import json
import logging
import os
import time
import traceback
import uuid
from collections import defaultdict
from concurrent.futures.process import BrokenProcessPool
from itertools import combinations
from typing import Any, Union

from shepherd_utils.broker import (
    add_task,
    mark_task_as_complete,
    refresh_lock,
    remove_lock,
    try_lock,
)
from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.db import (
    clear_ready_callback,
    get_message,
    get_message_sync,
    get_ready_callbacks,
    remove_callback_id,
    save_logs,
    save_message_sync,
)
from shepherd_utils.logger import QueryLogger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import filter_kgraph_orphans, get_tasks, merge_kgraph

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


def _normalize_query(query):
    """Build a normalized copy of a query graph for equivalence comparison.

    Returns shallow-copied node/edge dicts so we never mutate the caller's
    data; avoids the full ``copy.deepcopy`` the previous implementation paid
    on every call.
    """
    nq_nodes = {}
    for nid, node in query["nodes"].items():
        n = dict(node)
        if n.get("is_set") is False:
            n.pop("is_set", None)
        si = n.get("set_interpretation")
        if si == "BATCH" or si is None:
            n.pop("set_interpretation", None)
        if "constraints" in n and len(n["constraints"]) == 0:
            del n["constraints"]
        if "member_ids" in n and len(n["member_ids"]) == 0:
            del n["member_ids"]
        if n.get("ids") is None:
            n.pop("ids", None)
        if n.get("categories") is None:
            n.pop("categories", None)
        nq_nodes[nid] = n

    nq_edges = {}
    for eid, edge in query["edges"].items():
        e = dict(edge)
        if "attribute_constraints" in e and len(e["attribute_constraints"]) == 0:
            del e["attribute_constraints"]
        if "qualifier_constraints" in e and len(e["qualifier_constraints"]) == 0:
            del e["qualifier_constraints"]
        e.pop("knowledge_type", None)
        preds = e.get("predicates")
        if preds and any(p == "biolink:treats" for p in preds):
            e["predicates"] = [
                (
                    "biolink:treats_or_applied_or_studied_to_treat"
                    if p == "biolink:treats"
                    else p
                )
                for p in preds
            ]
        nq_edges[eid] = e

    return {"nodes": nq_nodes, "edges": nq_edges}


def queries_equivalent(query1, query2):
    """Compare 2 query graphs.  The nuisance is that there is flexiblity in e.g. whether there is a qualifier constraint
    as none or it's not in there or its an empty list.  And similar for is_set and is_set is False.
    """
    return _normalize_query(query1) == _normalize_query(query2)


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


def get_answer_node(query_graph: dict[str, Any]) -> Union[str, None]:
    """From the original query graph, get the answer node id."""
    answer_node = None
    qnodes = query_graph.get("nodes", {})
    for qnode_id, qnode in qnodes.items():
        if qnode.get("ids") is None:
            if answer_node is not None:
                # if there are multiple unpinned nodes
                return None
            answer_node = qnode_id
    return answer_node


def has_unique_nodes(result):
    """Given a result, return True if all nodes are unique, False otherwise"""
    seen = set()
    for qnode, knodes in result["node_bindings"].items():
        knode_ids = frozenset([knode["id"] for knode in knodes])
        if knode_ids in seen:
            return False
        seen.add(knode_ids)
    return True


def filter_repeated_nodes(response, logger: logging.Logger):
    """We have some rules that include e.g. 2 chemicals.
    We don't want responses in which those two are the same.
    If you have A-B-A-C then what shows up in the ui is B-A-C which makes no sense.
    """
    original_result_count = len(response["message"].get("results", []))
    if original_result_count == 0:
        return
    results = list(
        filter(lambda x: has_unique_nodes(x), response["message"]["results"])
    )
    response["message"]["results"] = results
    if len(results) != original_result_count:
        filter_kgraph_orphans(response, logger)


def get_promiscuous_qnodes(response):
    """We have some rules like A<-treats-B-part_of->C<-part_of-D.  Figure out if this
    qgraph is like that and return C if it is"""
    qgraph = response["message"]["query_graph"]
    if len(qgraph["edges"]) < 3:
        return []
    # for this to be a problem, we need 2 edges that share a subject or an object,
    # and have the same predicates and qualifiers.
    subjects = defaultdict(list)
    objects = defaultdict(list)
    for qedge_id, qedge in qgraph["edges"].items():
        subjects[qedge["subject"]].append(qedge_id)
        objects[qedge["object"]].append(qedge_id)
    center_nodes = []
    for nodelist in (subjects, objects):
        for node, edges in nodelist.items():
            if len(edges) < 2:
                continue
            for eid1, eid2 in combinations(edges, 2):
                e1 = qgraph["edges"][eid1]
                e2 = qgraph["edges"][eid2]
                if e1["predicates"] == e2["predicates"]:
                    if e1.get("qualifiers", []) == e2.get("qualifiers", []):
                        center_nodes.append(node)
    return center_nodes


def remove_promiscuous_knode_results(MAX_C, qnode, response):
    """Given a response and a qnode, look at all the results and count how many of the
    results have the same knode bound to that qnode.
    If that number is greater than MAX_C, remove those results."""
    still_going = True
    # This is written as a loop with the idea that once we've removed one
    # promiscuous node, it might require recalculating everything since the results
    # change. In retrospect, that might not be true because we are specifiying the
    # qnode. I'm still think it's possible (but perhaps unlikely) if there are
    # multiple knodes bound to the same qnode.
    while still_going:
        still_going = False
        # How many distinct results have the same bozo in this spot?
        prom_counter = defaultdict(list)
        for result_i, result in enumerate(response["message"]["results"]):
            for binding in result["node_bindings"][qnode]:
                knode = binding["id"]
                prom_counter[knode].append(result_i)
        # now figure out the most common knode
        max_knode = None
        max_count = 0
        for knode, mapped_result_indices in prom_counter.items():
            if len(mapped_result_indices) > max_count:
                max_knode = knode
                max_count = len(mapped_result_indices)
        # Now remove all the results with that knode (if it occurs in more than
        # MAX_C results)
        if max_count > MAX_C:
            still_going = True
            # These are the indices of the results that we want to remove
            mapped_result_indices = prom_counter[max_knode]
            # Remove them from right to left, otherwise the indices change on you
            for index in reversed(mapped_result_indices):
                del response["message"]["results"][index]


def filter_promiscuous_results(response, logger: logging.Logger):
    """We have some rules like A<-treats-B-part_of->C<-part_of-D.
    This is saying B treats A, and D is like B (because they are both part of C).
    This isn't the worst rule in the world, we find it statistically useful.  But,
    there are Cs that contain lllllooooootttttssss of stuff, and it creates a lot of
    bad results. Not only are they bad, but they are basically the same in terms of
    score, so we create a lot of ties. We are taking some approaches to fixing this
    in ranking, but really the results are just terrible, let's get rid of them,
    but distinguish cases where the rule is doing something interesting from when
    it is not. And note that "part_of" is not the only rule that follows this
    similarity-style pattern.   The difference is basically how many times C occurs.
    What we'd really like to do is not use promiscuous nodes in the C spot (or other
    places really).  But we don't have a promiscuity score for the nodes, and can't
    really get one.
    """
    # First, we need to know if we have too many results, and if it's the right
    # kind of query
    MAX_C = 10
    if len(response["message"]["results"]) < MAX_C:
        return
    prom_qnodes = get_promiscuous_qnodes(response)
    # This is a dictionary from bound knodes to the index of their result
    # There should only be one such node
    for qnode in prom_qnodes:
        # It's possible that there are multiple knodes that could be filtered.  But
        # when we filter out the first one then the indices of the rest will change.
        # So we need to do this one at a time.
        remove_promiscuous_knode_results(MAX_C, qnode, response)


def merge_messages(
    target: str,
    original_query_graph: dict[str, Any],
    response: dict[str, Any],
    new_response: dict[str, Any],
    logger: logging.Logger,
):
    pydantic_kgraph = {"nodes": {}, "edges": {}}
    source = f"infores:shepherd-{target}"
    filter_repeated_nodes(new_response, logger)
    filter_promiscuous_results(new_response, logger)
    for result_message in [response, new_response]:
        result_kgraph = (
            result_message["message"]["knowledge_graph"]
            if result_message["message"].get("knowledge_graph") is not None
            else {"nodes": {}, "edges": {}}
        )
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
    merged_aux = result["message"]["auxiliary_graphs"]
    for result_message in [response, new_response]:
        src_aux = result_message["message"].get("auxiliary_graphs")
        if not src_aux:
            continue
        # Reference adoption is safe: the input messages are discarded after
        # merging, so nothing else is going to mutate these structures.
        for aux_id, aux_dict in src_aux.items():
            existing = merged_aux.get(aux_id)
            if existing is None:
                merged_aux[aux_id] = aux_dict
                continue
            for key, val in aux_dict.items():
                if key in existing:
                    if isinstance(existing[key], list):
                        # combine both lists and then list/set it for uniqueness
                        existing[key] = list(set(existing[key] + val))
                    else:
                        logger.warning(
                            f"Message had an invalid aux graph property: {key}"
                        )
                else:
                    existing[key] = val

    # Determine type of message
    if "edges" in original_query_graph:
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
        if answer_node_id is None:
            # This was a direct lookup outside of a creative query, just return it
            merged_messages = new_response
        else:
            merged_messages = merge_results_by_node(
                target, result, answer_node_id, lookup_results
            )

        return merged_messages
    elif "paths" in original_query_graph:
        # Pathfinder query
        path_id, og_path = next(iter(original_query_graph["paths"].items()))
        subject_node_id = og_path.get("subject")
        object_node_id = og_path.get("object")
        if subject_node_id is None or object_node_id is None:
            raise KeyError("Missing either subject or object from path.")

        intermediate_category = None
        constraints = og_path.get("constraints") or []
        if len(constraints) > 0:
            intermediate_categories = (
                constraints[0].get("intermediate_categories") or []
            )
            if len(intermediate_categories) > 0:
                intermediate_category = intermediate_categories[0]

        kg_nodes = result["message"]["knowledge_graph"].get("nodes", {})
        kg_edges = result["message"]["knowledge_graph"].get("edges", {})

        aux_counter = 0
        score = 0
        analyses = []
        for new_result in new_response["message"]["results"]:
            path_edge_ids = set()
            for analysis in new_result.get("analyses", []):
                edge_bindings = analysis.get("edge_bindings", {})
                for qg_edge_key, bindings in edge_bindings.items():
                    for binding in bindings:
                        path_edge_ids.add(binding["id"])
            score = new_result.get("score")
            if not path_edge_ids:
                continue

            if (
                intermediate_category is not None
                and intermediate_category != "biolink:NamedThing"
            ):
                nb = new_result.get("node_bindings", {})
                pinned_ids = set()
                for pinned in (subject_node_id, object_node_id):
                    for binding in nb.get(pinned, []) or []:
                        pinned_ids.add(binding["id"])
                intermediate_node_ids = set()
                for edge_id in path_edge_ids:
                    edge = kg_edges.get(edge_id)
                    if edge is None:
                        continue
                    for node_id in (edge.get("subject"), edge.get("object")):
                        if node_id and node_id not in pinned_ids:
                            intermediate_node_ids.add(node_id)
                if not any(
                    intermediate_category
                    in (kg_nodes.get(nid, {}).get("categories") or [])
                    for nid in intermediate_node_ids
                ):
                    continue

            aux_id = f"a_{aux_counter}"
            aux_counter += 1

            # Add new aux graph to message
            result["message"]["auxiliary_graphs"][aux_id] = {
                "edges": list(path_edge_ids),
                "attributes": [],
            }

            analysis = {
                "resource_id": source,
                "path_bindings": {path_id: [{"id": aux_id}]},
            }
            if score is not None:
                analysis["score"] = score
            analyses.append(analysis)

        # --- Resolve the pinned node IDs from any old result's node_bindings ---
        # (They should all bind the same start/end IDs since they're pinned)
        if not analyses:
            result["message"]["results"] = []
            return result

        start_kg_id = None
        end_kg_id = None
        for new_result in new_response["message"]["results"]:
            nb = new_result.get("node_bindings", {})
            if subject_node_id in nb and nb[subject_node_id]:
                start_kg_id = nb[subject_node_id][0]["id"]
            if object_node_id in nb and nb[object_node_id]:
                end_kg_id = nb[object_node_id][0]["id"]
            if start_kg_id and end_kg_id:
                break

        if not start_kg_id or not end_kg_id:
            result["message"]["results"] = []
            return result

        # --- Assemble the single Pathfinder result ---
        pathfinder_result = {
            "node_bindings": {
                subject_node_id: [{"id": start_kg_id, "attributes": []}],
                object_node_id: [{"id": end_kg_id, "attributes": []}],
            },
            "analyses": analyses,
        }

        result["message"]["results"] = [pathfinder_result]

        return result

    raise TypeError("Unsupported query type.")


# ---------------------------------------------------------------------------
# Worker entry point
#
# Runs in a ProcessPoolExecutor worker. Fetches the three messages directly
# from Redis using the sync client, performs the merge, and writes the result
# back. Nothing large crosses the process boundary in either direction.
# ---------------------------------------------------------------------------


def merge_messages_by_ids(
    target: str,
    query_id: str,
    response_id: str,
    callback_ids: list[str],
    log_level: int = logging.INFO,
) -> tuple[list[str], list[dict]]:
    """Worker-side batched merge: fetch by id, fold every callback into the
    accumulating response, write back once. No payloads cross IPC.

    The query and the accumulating response are loaded a single time; each
    callback is folded into the in-memory accumulator, and the result is saved
    once. Folding sequentially is equivalent to merging the callbacks one at a
    time (which is how they were processed before), but avoids re-loading and
    re-saving the ever-growing response blob per callback.

    Returns a ``(merged, log_entries)`` tuple. ``merged`` is the list of
    callback ids that were actually folded in, so the caller knows which to
    clear from the ready index and the callbacks table (a single missing
    callback is skipped rather than aborting the whole batch). ``log_entries``
    is the list of formatted log records produced during the merge, oldest
    first -- this runs in a ProcessPoolExecutor child, so its logger can't be
    the parent's query logger; instead we attach a fresh ``QueryLogHandler``
    here and hand its contents back across the process boundary for the parent
    to fold into the query's log list.
    """
    # A logger.getLogger call in a child returns the same object for the whole
    # process life, so attach a call-scoped handler and remove it in finally --
    # otherwise handlers would accumulate across the child's successive tasks
    # and leak one query's logs into the next.
    query_log_handler = QueryLogger().log_handler
    worker_logger = logging.getLogger(f"merge_message.worker.{os.getpid()}")
    worker_logger.setLevel(log_level)
    worker_logger.addHandler(query_log_handler)
    try:
        try:
            original_query = get_message_sync(query_id)
            accumulator = get_message_sync(response_id)
        except KeyError as e:
            worker_logger.error(f"Missing message in worker: {e}")
            raise

        original_query_graph = original_query["message"]["query_graph"]
        merged: list[str] = []
        for callback_id in callback_ids:
            try:
                callback_response = get_message_sync(callback_id)
            except KeyError:
                worker_logger.error(
                    f"Missing callback {callback_id} while folding; skipping."
                )
                continue
            accumulator = merge_messages(
                target,
                original_query_graph,
                accumulator,
                callback_response,
                worker_logger,
            )
            merged.append(callback_id)

        if merged:
            save_message_sync(response_id, accumulator)
        # contents() is newest-first (emit appendlefts); hand back oldest-first
        # so the parent can appendleft them in order and keep the queue's
        # newest-first invariant.
        log_entries = list(query_log_handler.contents())
        log_entries.reverse()
        return merged, log_entries
    finally:
        worker_logger.removeHandler(query_log_handler)


def merge_messages_by_id(
    target: str,
    query_id: str,
    response_id: str,
    callback_id: str,
    log_level: int = logging.INFO,
) -> bool:
    """Worker-side merge of a single callback (thin delegate to the batched
    path). Retained for callers/tests that merge one callback at a time."""
    merged, _ = merge_messages_by_ids(
        target, query_id, response_id, [callback_id], log_level
    )
    return bool(merged)


async def _reenqueue_wake_task(task, logger):
    """Put a fresh merge_message wake task back on the stream.

    Used when this worker can't make progress on a callback right now (the
    query's lock is held by someone else, or a merge failed). The callback's
    entry in the ready index is left intact; the new wake task simply drives
    another drain attempt later, so callbacks are retried rather than dropped.
    """
    fields = {k: v for k, v in task[1].items() if k != "_started_at"}
    await add_task(STREAM, fields, logger)


async def poll_for_tasks():
    loop = asyncio.get_running_loop()
    # Size by the pod's actual CPU allocation (cgroup limit), not os.cpu_count()
    # which reports the whole node's cores. This one value drives both the pool
    # and the in-flight task limit below: each merge runs a child that loads the
    # growing response blob, so pool size == concurrency bounds peak memory.
    # POOL_MAX_WORKERS overrides.
    max_workers = resolve_pool_workers(TASK_LIMIT, logging.getLogger(STREAM))
    logging.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    # Shared self-healing pool: spawn-context executor that replaces itself in
    # place on a BrokenProcessPool (same implementation the aragorn.omnicorp /
    # aragorn.score / arax.rank workers use). run() swaps the dead pool before
    # re-raising, so the except block below just does the merge-specific cleanup.
    pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="merge_message process pool",
        task_timeout=settings.pool_task_timeout_sec,
    )

    async def _clear_batch(response_id, callback_ids, logger):
        """Drop a processed batch from the ready index and callbacks table.

        Every callback we attempted is removed (not just the ones that merged)
        so a callback whose payload has vanished can't wedge the drain loop.
        """
        await asyncio.gather(
            *(clear_ready_callback(response_id, cb, logger) for cb in callback_ids)
        )
        await asyncio.gather(*(remove_callback_id(cb, logger) for cb in callback_ids))

    def _ingest_merge_logs(logger, entries):
        """Fold the merge child's log records into this task's query logger.

        The child formatted them into an isolated handler; drop them into this
        logger's query handler so the ``save_logs`` in ``finally`` flushes them
        to the query's log list alongside the parent's own lines.
        """
        if not entries:
            return
        handler = next(
            (
                h
                for h in logger.handlers
                if getattr(h, "name", None) == "query_log_handler"
            ),
            None,
        )
        if handler is not None:
            handler.ingest(entries)

    async def process_query(task, parent_ctx, logger, limiter):
        start = time.time()
        query_id = task[1]["query_id"]
        response_id = task[1]["response_id"]
        callback_id = task[1]["callback_id"]
        target = task[1]["target"]
        log_level = int(task[1].get("log_level", logging.INFO))
        drained = 0
        try:
            with tracer.start_as_current_span(STREAM, context=parent_ctx) as span:
                span.set_attribute("callback_id", callback_id)
                span.set_attribute("response_id", response_id)

                # Non-blocking: never wait on the lock. The worker that holds it
                # drains the whole query, so a loser has nothing useful to add.
                got_lock = await try_lock(response_id, CONSUMER, logger)
                if not got_lock:
                    # Someone else holds this query's lock and drains its ready
                    # set to empty, so our callback (added to the set before this
                    # wake task was enqueued) will be merged by them. Just ack and
                    # move on -- no re-enqueue. The holder does one final ready-set
                    # check after releasing the lock (below) to catch a callback
                    # that lands in the narrow window past its last drain pass, so
                    # nothing is stranded. This replaces the old per-loser
                    # re-enqueue, which spun a wake task per contended callback
                    # every merge_contention_backoff and flooded the logs with
                    # near-identical "Doing task" lines during a single merge.
                    logger.debug(
                        f"[{callback_id}] Lock busy; holder will drain it. Acking."
                    )
                    return

                logger.info(f"[{callback_id}] Obtained lock for {response_id}.")
                # Sanity check: if the original query is gone, every ready
                # callback for it is undeliverable -- clean them all up.
                try:
                    await get_message(query_id, logger)
                except KeyError:
                    logger.error(
                        f"Failed to get original query for {query_id}. "
                        "Discarding ready callbacks."
                    )
                    orphans = await get_ready_callbacks(response_id, logger)
                    await _clear_batch(response_id, orphans, logger)
                    await remove_lock(response_id, CONSUMER, logger)
                    return

                lock_time = time.time()
                # Drain the query to empty: one load + one save per iteration.
                # Re-reading the set each pass sweeps up callbacks that arrived
                # while we were merging, so the holder does all of this query's
                # ready work.
                try:
                    while True:
                        ready = await get_ready_callbacks(response_id, logger)
                        if not ready:
                            break
                        if settings.merge_max_fold > 0:
                            ready = ready[: settings.merge_max_fold]
                        merged, merge_logs = await pool.run(
                            loop,
                            merge_messages_by_ids,
                            target,
                            query_id,
                            response_id,
                            ready,
                            log_level,
                        )
                        _ingest_merge_logs(logger, merge_logs)
                        await _clear_batch(response_id, ready, logger)
                        drained += len(merged)
                        # Keep our lock alive across a long multi-pass drain.
                        await refresh_lock(response_id, CONSUMER, 45000, logger)
                except BrokenProcessPool:
                    # pool.run already swapped in a fresh executor; here we just
                    # release the lock and re-enqueue so the callback is retried.
                    logger.error(f"[{callback_id}] Process pool broken; re-enqueuing.")
                    await remove_lock(response_id, CONSUMER, logger)
                    await _reenqueue_wake_task(task, logger)
                    span.set_attribute("drained_callbacks", drained)
                    return
                except Exception:
                    logger.error(
                        f"[{callback_id}] Error merging messages: "
                        f"{traceback.format_exc()}"
                    )
                    await remove_lock(response_id, CONSUMER, logger)
                    await _reenqueue_wake_task(task, logger)
                    span.set_attribute("drained_callbacks", drained)
                    return

                span.set_attribute("drained_callbacks", drained)
                logger.info(
                    f"[{callback_id}] Drained {drained} callback(s) in "
                    f"{time.time() - lock_time:.2f}s"
                )
                await remove_lock(response_id, CONSUMER, logger)
                # Close the race where a callback landed in the ready set after
                # our final drain pass read it empty but before we released the
                # lock: that callback's own wake task would have found the lock
                # held and dropped (losers no longer re-enqueue). Now that the
                # lock is free, re-check and kick exactly one wake if anything
                # remains so the late arrival still gets merged. One conditional
                # re-enqueue replaces the old storm of per-loser re-enqueues.
                try:
                    leftover = await get_ready_callbacks(response_id, logger)
                except Exception:
                    leftover = []
                if leftover:
                    logger.debug(
                        f"[{callback_id}] {len(leftover)} callback(s) arrived "
                        "post-drain; kicking one wake task."
                    )
                    await _reenqueue_wake_task(task, logger)
        except Exception as e:
            logger.error(
                f"Task {task[0]} failed with unhandled error: {e}", exc_info=True
            )
        finally:
            try:
                await mark_task_as_complete(STREAM, GROUP, task[0], logger)
            except Exception as e:
                logger.error(f"Task {task[0]}: Failed to wrap up task: {e}")
            logger.info(f"Finished task {task[0]} in {time.time() - start:.2f}s")
            # Unlike normal workers, this worker hand-rolls its lifecycle and
            # never calls wrap_up_task, so nothing else persists its logs. Flush
            # them here (keyed by response_id, the key finish_query reads) so the
            # parent's own lines *and* the folded-in merge-child logs make it
            # into the query's log list.
            await save_logs(response_id, logger)
            limiter.release()

    inflight = set()
    while True:
        try:
            # Dispatch each wake task concurrently. Distinct queries never share
            # a lock, so their merges run in parallel on the process pool; the
            # task_limiter semaphore (sized to max_workers) bounds concurrency.
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, max_workers
            ):
                t = asyncio.create_task(
                    process_query(task, parent_ctx, logger, limiter)
                )
                inflight.add(t)
                t.add_done_callback(inflight.discard)
        except asyncio.CancelledError:
            logging.info("Poll loop cancelled, shutting down.")
            for t in inflight:
                t.cancel()
            pool.shutdown()
            return
        except Exception as e:
            logging.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
