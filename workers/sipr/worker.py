"""SIPR (Set-Input Page Rank) module."""

import asyncio
import logging
import time
import uuid
from copy import deepcopy

import httpx
import networkx as nx

from shepherd_utils.config import settings
from shepherd_utils.db import (
    get_message,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "sipr"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
MAX_QUERY_TIME = 2400
tracer = setup_tracer(STREAM)


async def get_neighborhood(id_list: list[str], depth: int, logger):
    used = set()
    candidate_list = deepcopy(id_list)
    trapi_responses = []
    for hop_num in range(depth):
        query_nodes = [node_id for node_id in candidate_list if node_id not in used]
        if len(query_nodes) == 0:
            break
        used.update(query_nodes)
        i = 0
        curie_num = 100
        while i < len(query_nodes):
            step = write_trapi(query_nodes[i : i + curie_num], hop_num)
            response = await run_trapi(step, logger)
            candidate_list, filtered_response = get_nodes(response, query_nodes, logger)
            trapi_responses.append(filtered_response)
            i += curie_num
    return trapi_responses


def get_nodes(response, query_nodes, logger):
    all_nodes = set()
    node_prefixes = set()
    initial_filtered_graph = {
        "message": {
            "knowledge_graph": {
                "nodes": {},
                "edges": {},
            }
        }
    }
    for edge_id, edge in response["message"]["knowledge_graph"]["edges"].items():
        all_nodes.add(edge["subject"])
        all_nodes.add(edge["object"])
        if (
            edge["predicate"] != "biolink:subclass_of"
            and edge["predicate"] != "biolink:related_to"
        ):
            if not edge["subject"].startswith("HP:") or not edge["object"].startswith(
                "HP:"
            ):
                initial_filtered_graph["message"]["knowledge_graph"]["edges"][
                    edge_id
                ] = edge
                initial_filtered_graph["message"]["knowledge_graph"]["nodes"][
                    edge["subject"]
                ] = response["message"]["knowledge_graph"]["nodes"][edge["subject"]]
                initial_filtered_graph["message"]["knowledge_graph"]["nodes"][
                    edge["object"]
                ] = response["message"]["knowledge_graph"]["nodes"][edge["object"]]
    ppr = distribute_weights([initial_filtered_graph], query_nodes, logger)
    node_scores = list(ppr.items())
    node_scores = sorted(node_scores, key=lambda x: x[1], reverse=True)
    filtered_nodes = list(filter(lambda x: not x[0].startswith("HP:"), node_scores))[
        :15
    ]
    filtered_nodes = {node_id: score for node_id, score in filtered_nodes}
    final_filtered_graph = {
        "message": {
            "knowledge_graph": {
                "nodes": {},
                "edges": {},
            }
        }
    }
    for edge_id, edge in initial_filtered_graph["message"]["knowledge_graph"][
        "edges"
    ].items():
        if filtered_nodes.get(edge["subject"]) or filtered_nodes.get(edge["object"]):
            node_prefixes.add(edge["subject"].split(":")[0])
            node_prefixes.add(edge["object"].split(":")[0])
            final_filtered_graph["message"]["knowledge_graph"]["edges"][edge_id] = edge
            final_filtered_graph["message"]["knowledge_graph"]["nodes"][
                edge["subject"]
            ] = response["message"]["knowledge_graph"]["nodes"][edge["subject"]]
            final_filtered_graph["message"]["knowledge_graph"]["nodes"][
                edge["object"]
            ] = response["message"]["knowledge_graph"]["nodes"][edge["object"]]
    return list(filtered_nodes.keys()), final_filtered_graph


def write_trapi(id_list, hop_num):
    object_categories = ["biolink:NamedThing"]
    if hop_num == 1:
        object_categories = [
            "biolink:ChemicalEntity",
            "biolink:Disease",
            "biolink:BiologicalProcessOrActivity",
            "biolink:Gene",
            "biolink:Protein",
            "biolink:OrganismalEntity",
        ]
    qg = {
        "nodes": {
            "n0": {"ids": id_list},
            "n1": {
                "categories": object_categories,
            },
        },
        "edges": {
            "e0": {
                "subject": "n0",
                "object": "n1",
            }
        },
    }
    query = {
        "message": {
            "query_graph": qg,
            "knowledge_graph": {
                "nodes": {},
                "edges": {},
            },
            "results": [],
            "auxiliary_graphs": {},
        },
        "parameters": {
            "timeout": 3600,
        },
    }
    return query


async def run_trapi(query, logger):
    response = deepcopy(query)
    try:
        async with httpx.AsyncClient(timeout=3600) as client:
            response = await client.post(
                settings.sync_kg_retrieval_url,
                json=query,
            )

        response.raise_for_status()
        response = response.json()
    except Exception as e:
        logger.error(f"Failed to get a good response from kg retrieval: {str(e)}")
    return response


def distribute_weights(trapi_responses, target_nodes, logger):
    G = nx.DiGraph()
    for response in trapi_responses:
        kg = response["message"]["knowledge_graph"]

        # %%

        edges = [
            (e["subject"], e["object"], {"id": eid, "edge": e})
            for eid, e in kg["edges"].items()
        ]

        G.add_edges_from(edges)

    # %%
    # Personalization vector (bias toward some nodes)
    # Example: bias toward node "A"
    personalization = None
    if len(target_nodes) > 0:
        personalization = {node: 0.0 for node in G.nodes()}
        for target_node_id in target_nodes:
            personalization[target_node_id] = 1.0 / len(target_nodes)

    # Compute personalized PageRank
    ppr = nx.pagerank(G, alpha=0.85, personalization=personalization)

    return ppr


async def sipr(task, logger: logging.Logger):
    start = time.time()
    workflow = [
        {"id": "sipr"},
        {"id": "sort_results_score"},
    ]
    try:
        # given a task, get the message from the db
        logger.info("Getting message from db")
        query_id = task[1]["query_id"]
        response_id = task[1]["response_id"]
        message = await get_message(query_id, logger)

        # graph retrieval
        # TODO: make this smarter
        nodes = list(message["message"]["query_graph"]["nodes"]["SN"]["ids"])
        logger.info(f"Getting neighborhood for {nodes}")
        trapi_responses = await get_neighborhood(nodes, 2, logger)

        # distribute weights
        logger.info("Distributing weights")
        ppr = distribute_weights(trapi_responses, nodes, logger)
        logger.info("Sorting page rank...")
        node_scores = list(ppr.items())
        node_scores = sorted(node_scores, key=lambda x: x[1], reverse=True)

        # make final trapi message
        logger.info("Making final message")
        final_message = {
            "message": {
                "query_graph": deepcopy(message["message"]["query_graph"]),
                "knowledge_graph": {
                    "nodes": {},
                    "edges": {},
                },
                "results": [],
                "auxiliary_graphs": {},
            },
        }
        for in_node in nodes:
            kg_node = {
                "categories": [],
                "name": in_node,
            }
            for trapi_response in trapi_responses:
                # grab kg node from trapi messages
                if (
                    trapi_response["message"]["knowledge_graph"]["nodes"].get(in_node)
                    is not None
                ):
                    kg_node = trapi_response["message"]["knowledge_graph"]["nodes"][
                        in_node
                    ]
                    break
            final_message["message"]["knowledge_graph"]["nodes"][in_node] = kg_node
        node_bindings = [
            {
                "attributes": [],
                "id": node_id,
            }
            for node_id in nodes
        ]
        for node, score in node_scores:
            if score < 0.001:
                # throw out any nodes with a too low score
                continue
            kg_node = {
                "categories": [],
                "name": node,
            }
            for trapi_response in trapi_responses:
                # grab kg node from trapi messages
                if (
                    trapi_response["message"]["knowledge_graph"]["nodes"].get(node)
                    is not None
                ):
                    kg_node = trapi_response["message"]["knowledge_graph"]["nodes"][
                        node
                    ]
                    break
            final_message["message"]["knowledge_graph"]["nodes"][node] = kg_node
            new_edge_ids = []
            for in_node in nodes:
                new_edge_id = str(uuid.uuid4())[:8]
                final_message["message"]["knowledge_graph"]["edges"][new_edge_id] = {
                    "subject": in_node,
                    "predicate": "biolink:related_to",
                    "object": node,
                    "attributes": [],
                }
                new_edge_ids.append(new_edge_id)
            edge_bindings = [
                {
                    "attributes": [],
                    "id": edge_id,
                }
                for edge_id in new_edge_ids
            ]
            final_message["message"]["results"].append(
                {
                    "analyses": [
                        {
                            "edge_bindings": {
                                "e0": edge_bindings,
                            },
                            "resource_id": "infores:shepherd_sipr",
                            "score": score,
                            "support_graphs": [],
                        }
                    ],
                    "node_bindings": {
                        "SN": node_bindings,
                        "ON": [
                            {
                                "attributes": [],
                                "id": node,
                            },
                        ],
                    },
                }
            )

        await save_message(response_id, final_message, logger)

    except Exception as e:
        logger.error(f"Something bad happened! {e}")

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)

    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await sipr(task, logger)
    except Exception as e:
        logger.error(f"Something went wrong: {e}")
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
