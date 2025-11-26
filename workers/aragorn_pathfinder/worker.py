"""Aragorn ARA Pathfinder module."""

import asyncio
import gzip
import hashlib
import json
import logging
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import List

import httpx
import networkx
import redis

from shepherd_utils.config import settings
from shepherd_utils.db import (
    get_message,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import (
    get_tasks,
    merge_kgraph,
    recursive_get_edge_support_graphs,
    wrap_up_task,
)

# Queue name
STREAM = "aragorn.pathfinder"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)

NUM_TOTAL_HOPS = 4
TOTAL_PUBS = 27840000
CURIE_PRUNING_LIMIT = 50
LIT_CO_FACTOR = 10000
INFORMATION_CONTENT_THRESHOLD = 85

blocklist = []
with open(Path(__file__).parent / "blocklist.json", "r", encoding="utf-8") as f:
    blocklist = json.load(f)


def get_the_pmids(curies: List[str]):
    """Returns a list of pmids for papers that mention all curies in list"""
    r = redis.Redis(
        host=settings.pathfinder_redis_host,
        port=settings.pathfinder_redis_port,
        db=settings.pathfinder_pmid_db,
        password=settings.pathfinder_redis_password,
    )
    curie_pmids = []
    for curie in curies:
        pmids = r.get(curie)
        if pmids is None:
            pmids = []
        else:
            pmids = json.loads(gzip.decompress(pmids))
        curie_pmids.append(pmids)
    answer = list(set.intersection(*map(set, curie_pmids)))
    return answer


def get_the_curies(pmid: str):
    """Returns a list of all curies in the paper corresponding to the pmid."""
    r = redis.Redis(
        host=settings.pathfinder_redis_host,
        port=settings.pathfinder_redis_port,
        db=settings.pathfinder_curies_db,
        password=settings.pathfinder_redis_password,
    )
    curies = r.get(pmid)
    if curies is None:
        curies = []
    else:
        curies = json.loads(gzip.decompress(curies))
    answer = list(curies)
    return answer


async def generate_from_lookup(message, logger: logging.Logger):
    """Generates knowledge graphs from lookup."""
    try:
        async with httpx.AsyncClient(timeout=3600) as client:
            logger.debug(f"Sending query to {settings.sync_kg_retrieval_url}")
            lookup_response = await client.post(
                url=settings.sync_kg_retrieval_url,
                json=message,
            )
            lookup_response.raise_for_status()
            lookup_response = lookup_response.json()
    except Exception:
        lookup_response = {}
    return lookup_response.get("message", {})


async def get_normalized_curies(curies, logger: logging.Logger):
    """Gives us normalized curies that we can look up in our database, assuming
    the database is also properly normalized."""
    async with httpx.AsyncClient(timeout=900) as client:
        try:
            normalizer_response = await client.post(
                url=settings.node_norm + "get_normalized_nodes",
                json={"curies": list(curies), "conflate": True, "description": False, "drug_chemical_conflate": True},
            )
            normalizer_response.raise_for_status()
            return normalizer_response.json()
        except Exception:
            logger.info("Failed to get a response from node norm")


async def shadowfax(task, logger: logging.Logger):
    """Processes pathfinder queries. This is done by using literature
    co-occurrence to find nodes that occur in publications with our input
    nodes, then finding paths that connect our input nodes through these
    intermediate nodes."""
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    response_id = task[1]["response_id"]
    message = await get_message(query_id, logger)
    parameters = message.get("parameters") or {}
    parameters["timeout"] = parameters.get("timeout", settings.lookup_timeout)
    parameters["tiers"] = parameters.get("tiers") or [0]
    message["parameters"] = parameters

    qgraph = message["message"]["query_graph"]
    pinned_node_ids_set = set()
    pinned_node_keys = []
    for node_key, node in qgraph["nodes"].items():
        pinned_node_keys.append(node_key)
        if node.get("ids", None) is not None:
            pinned_node_ids_set.add(node["ids"][0])
    if len(pinned_node_ids_set) != 2:
        logger.error("Pathfinder queries require two pinned nodes.")
        return message, 500
    pinned_node_ids = list(pinned_node_ids_set)

    intermediate_categories = []
    path_key = next(iter(qgraph["paths"].keys()))
    qpath = qgraph["paths"][path_key]
    if qpath.get("constraints", None) is not None:
        constraints = qpath["constraints"]
        if len(constraints) > 1:
            logger.error("Pathfinder queries do not support multiple constraints.")
            return message, 500
        if len(constraints) > 0:
            intermediate_categories = constraints[0].get("intermediate_categories", None) or []
        if len(intermediate_categories) > 1:
            logger.error("Pathfinder queries do not support multiple intermediate categories")
            return message, 500
    else:
        intermediate_categories = ["biolink:NamedThing"]

    normalized_pinned_ids = await get_normalized_curies(pinned_node_ids, logger)
    if normalized_pinned_ids is None:
        normalized_pinned_ids = {}

    source_node = normalized_pinned_ids.get(pinned_node_ids[0], {"id": {"identifier": pinned_node_ids[0]}})["id"]["identifier"]
    source_category = normalized_pinned_ids.get(pinned_node_ids[0], {"type": ["biolink:NamedThing"]})["type"][0]
    source_equivalent_ids = [i["identifier"] for i in normalized_pinned_ids.get(pinned_node_ids[0], {"equivalent_identifiers": []})["equivalent_identifiers"]]
    target_node = normalized_pinned_ids.get(pinned_node_ids[1], {"id": {"identifier": pinned_node_ids[1]}})["id"]["identifier"]
    target_equivalent_ids = [i["identifier"] for i in normalized_pinned_ids.get(pinned_node_ids[1], {"equivalent_identifiers": []})["equivalent_identifiers"]]
    target_category = normalized_pinned_ids.get(pinned_node_ids[1], {"type": ["biolink:NamedThing"]})["type"][0]

    # Find shared publications between input nodes
    source_pubs = len(get_the_pmids([source_node]))
    target_pubs = len(get_the_pmids([target_node]))
    pairwise_pubs = get_the_pmids([source_node, target_node])
    if source_pubs == 0 or target_pubs == 0 or len(pairwise_pubs) == 0:
        logger.info("No publications found.")
        return message, 200

    # Find other nodes from those shared publications
    curies = set()
    for pub in pairwise_pubs:
        curie_list = get_the_curies(pub)
        for curie in curie_list:
            if curie not in [source_node, target_node] and curie not in source_equivalent_ids and curie not in target_equivalent_ids:
                curies.add(curie)

    if len(curies) == 0:
        logger.info("No curies found.")
        return message, 200

    normalizer_response = await get_normalized_curies(list(curies), logger)
    if normalizer_response is None:
        logger.error("Failed to get a good response from Node Normalizer")
        return message, 500

    curie_info = defaultdict(dict)
    for curie, normalizer_info in normalizer_response.items():
        if normalizer_info:
            if (normalizer_info.get("information_content", 101) > INFORMATION_CONTENT_THRESHOLD) and curie not in blocklist:
                curie_info[curie]["categories"] = normalizer_info.get("type", ["biolink:NamedThing"])
                cooc = len(get_the_pmids([curie, source_node, target_node]))
                num_pubs = len(get_the_pmids([curie]))
                curie_info[curie]["pubs"] = num_pubs
                curie_info[curie]["score"] = max(
                    0,
                    ((cooc / TOTAL_PUBS) - (source_pubs / TOTAL_PUBS) * (target_pubs / TOTAL_PUBS) * (num_pubs / TOTAL_PUBS))
                )

    # Find the nodes with most significant co-occurrence
    pruned_curies = sorted(curie_info.keys(), key=lambda x: curie_info[x]["score"])[:CURIE_PRUNING_LIMIT]

    node_category_mapping = defaultdict(list)
    node_category_mapping[source_category].append(source_node)
    node_category_mapping[target_category].append(target_node)
    for curie in pruned_curies:
        node_category_mapping[curie_info[curie]["categories"][0]].append(curie)

    lookup_nodes = {}
    for category, category_curies in node_category_mapping.items():
        lookup_nodes[category.removeprefix("biolink:")] = {"ids": category_curies, "categories": [category]}

    # Create queries matching each category to each other
    lookup_queries = []
    for subject_index, subject_category in enumerate(lookup_nodes.keys()):
        for object_category in list(lookup_nodes.keys())[(subject_index + 1) :]:
            lookup_edge = {"subject": subject_category, "object": object_category, "predicates": ["biolink:related_to"]}
            m = {
                "message": {
                    "query_graph": {
                        "nodes": {subject_category: lookup_nodes[subject_category], object_category: lookup_nodes[object_category]},
                        "edges": {"e0": lookup_edge},
                    }
                },
                "parameters": {
                    "timeout": 120,
                    "tiers": [0]
                },
            }
            lookup_queries.append(m)
    lookup_messages = []
    logger.debug(f"Sending {len(lookup_queries)} requests to lookup.")
    for lookup_message in await asyncio.gather(*[generate_from_lookup(lookup_query, logger) for lookup_query in lookup_queries]):
        if lookup_message:
            lookup_message["query_graph"] = {"nodes": {}, "edges": {}}
            lookup_messages.append(lookup_message)
    logger.debug(f"Received {len(lookup_messages)} responses from lookup.")

    merged_kgraph = {"nodes": {}, "edges": {}}
    merged_aux_graphs = {}
    for lookup_message in lookup_messages:
        # Build graph from results to avoid subclass loops
        # Results do not concatenate when they have different qnode ids
        try:
            logger.debug(f"Got back {len(lookup_message.get('results', 0))} results.")
            merged_kgraph = merge_kgraph(merged_kgraph, lookup_message["knowledge_graph"], logger)
            merged_aux_graphs.update(lookup_message["auxiliary_graphs"])
        except KeyError as e:
            logger.error(f"Failed to merge message: {lookup_message}: {e}")

    # logger.info(f"Continuing with {len(merged_kgraph['nodes'])} nodes")
    non_support_edges = []
    for edge_key, edge in merged_kgraph["edges"].items():
        add_edge = True
        for support_graph in merged_aux_graphs.values():
            if edge_key in support_graph and edge["predicate"] == "biolink:subclass_of":
                # see if we can get paths from non subclass edges used in support graphs
                add_edge = False
        if add_edge:
            non_support_edges.append(edge_key)

    # logger.info(f"Continuing with {len(non_support_edges)} non support edges")

    # Build a large kg and find paths
    path_graph = networkx.Graph()
    path_graph.add_nodes_from([source_node, target_node])
    for edge_key in non_support_edges:
        edge = merged_kgraph["edges"][edge_key]
        e_subject = edge["subject"]
        e_object = edge["object"]
        if path_graph.has_edge(e_subject, e_object):
            path_graph[e_subject][e_object]["keys"].append(edge_key)
        else:
            path_graph.add_edge(e_subject, e_object, keys=[edge_key])

    paths = networkx.all_simple_paths(path_graph, source_node, target_node, NUM_TOTAL_HOPS)
    num_paths = 0
    result_paths = []
    for path in paths:
        num_paths += 1
        fits_constraint = False
        for curie in path:
            if curie not in [source_node, target_node]:
                # Handles constraints, behavior may change depending decision
                # handling multiple constraints
                if intermediate_categories[0] in merged_kgraph["nodes"].get(curie, {}).get("categories", []):
                    fits_constraint = True
        if fits_constraint:
            result_paths.append(path)

    # logger.info(f"Got {num_paths} paths.")

    result = {
        "node_bindings": {pinned_node_keys[0]: [{"id": source_node, "attributes": []}], pinned_node_keys[1]: [{"id": target_node, "attributes": []}]},
        "analyses": [],
    }

    for path in result_paths:
        aux_edges = []
        aux_edges_keys = []
        path_edges, path_support_graphs, path_nodes = set(), set(), set()
        support_node_graph_mapping = defaultdict(set)
        support_node_edge_mapping = defaultdict(set)
        missing_hop = False
        hop_count = 0
        hop_edge_map = defaultdict(set)
        for i, node in enumerate(path[:-1]):
            hop_count += 1
            next_node = path[i + 1]
            single_aux_edges = set()
            for kedge_key in path_graph[node][next_node]["keys"]:
                single_edges, single_support_graphs, single_nodes = set(), set(), set()
                # get nodes and edges from path
                try:
                    single_edges, single_support_graphs, single_nodes = recursive_get_edge_support_graphs(
                        kedge_key,
                        single_edges,
                        single_support_graphs,
                        merged_kgraph["edges"],
                        merged_aux_graphs,
                        single_nodes,
                    )
                except KeyError as e:
                    logger.warning(e)
                    continue
                for single_support_graph in single_support_graphs:
                    # map support graphs to component nodes
                    for edge_id in merged_aux_graphs[single_support_graph]["edges"]:
                        support_node_graph_mapping[merged_kgraph["edges"][edge_id]["subject"]].add(single_support_graph)
                        support_node_graph_mapping[merged_kgraph["edges"][edge_id]["object"]].add(single_support_graph)
                for edge_id in single_edges:
                    support_node_edge_mapping[merged_kgraph["edges"][edge_id]["subject"]].add(edge_id)
                    support_node_edge_mapping[merged_kgraph["edges"][edge_id]["object"]].add(edge_id)
                    hop_edge_map[hop_count].add(edge_id)
                check = True
                for path_node in single_nodes:
                    if path_node != node and path_node in path_nodes:
                        # if a node is repeated, remove all associated edges from path
                        check = False
                        for single_support_graph in support_node_graph_mapping[path_node]:
                            if single_support_graph in path_support_graphs:
                                path_support_graphs.remove(single_support_graph)
                        for edge_id in support_node_edge_mapping[path_node]:
                            if edge_id in path_edges:
                                path_edges.remove(edge_id)
                            if edge_id in aux_edges:
                                aux_edges.remove(edge_id)
                            for hop_edges in hop_edge_map.values():
                                if edge_id in hop_edges:
                                    hop_edges.remove(edge_id)

                if check:
                    path_edges.update(single_edges)
                    path_support_graphs.update(single_support_graphs)
                    # Don't add the current node in since we have more edges for this hop
                    path_nodes.update({single_node for single_node in single_nodes if (single_node != node and single_node != next_node)})
                    single_aux_edges.add(kedge_key)

            # Now add node in to check for repeats later
            path_nodes.add(node)

            # check if we have completely removed all edges from one of the hops in a path
            if len(single_aux_edges) == 0:
                missing_hop = True
            for hop_edges in hop_edge_map.values():
                if len(hop_edges) == 0:
                    missing_hop = True

            if missing_hop:
                break
            aux_edges.extend(single_aux_edges)

        # If a hop is missing, path is no longer valid, cannot be added
        if missing_hop:
            continue

        sha256 = hashlib.sha256()
        for x in set(aux_edges):
            sha256.update(bytes(x, encoding="utf-8"))
        aux_graph_key = sha256.hexdigest()
        if aux_graph_key not in aux_edges_keys:
            merged_aux_graphs[aux_graph_key] = {"edges": list(aux_edges), "attributes": []}
            aux_edges_keys.append(aux_graph_key)

        analysis = {
            "resource_id": "infores:aragorn",
            "path_bindings": {
                path_key: [{"id": aux_graph_key, "attributes": []}],
            },
        }
        result["analyses"].append(analysis)

    result_message = {
        "message": {
            "query_graph":  message["message"]["query_graph"],
            "knowledge_graph": merged_kgraph,
            "results": [result],
            "auxiliary_graphs": merged_aux_graphs
        }
    }

    await save_message(response_id, result_message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Task took {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await shadowfax(task, logger)
    finally:
        span.end()
        limiter.release()


async def poll_for_tasks():
    async for task, parent_ctx, logger, limiter in get_tasks(
        STREAM, GROUP, CONSUMER, TASK_LIMIT
    ):
        asyncio.create_task(process_task(task, parent_ctx, logger, limiter))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
