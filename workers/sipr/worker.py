"""SIPR (Set-Input Page Rank) module."""

import asyncio
import json
import logging
import uuid
from copy import deepcopy

import httpx
import networkx as nx

from shepherd_utils.config import settings
from shepherd_utils.db import (
    get_message,
    save_response,
)
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, run_task_lifecycle
from shepherd_utils.trapi import make_binding

# Queue name
STREAM = "sipr"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
MAX_QUERY_TIME = 2400
#: Provenance for the edges SIPR infers, matching merge_message's
#: ``infores:shepherd-{target}`` convention.
SIPR_INFORES = "infores:shepherd-sipr"
FALLBACK_CATEGORIES = ["biolink:NamedThing"]
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)


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
    response_kg = response["message"].get("knowledge_graph") or {}
    for edge_id, edge in (response_kg.get("edges") or {}).items():
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
                "predicates": ["biolink:related_to"],
            }
        },
    }
    # A TRAPI 2.0 query: just the query graph (an empty auxiliary_graphs is
    # invalid, and a query has no business carrying results).
    query = {
        "message": {
            "query_graph": qg,
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
        kg = response["message"].get("knowledge_graph") or {"edges": {}}

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


def make_sipr_edge(subject: str, obj: str) -> dict:
    """A TRAPI 2.0 knowledge-graph edge for a SIPR inference.

    SIPR infers the relationship from personalized PageRank over the retrieved
    neighborhood, so the edge is a computational-model prediction, and SIPR is
    its primary knowledge source.
    """
    return {
        "subject": subject,
        "predicate": "biolink:related_to",
        "object": obj,
        "knowledge_level": "prediction",
        "agent_type": "computational_model",
        "sources": [
            {
                "resource_id": SIPR_INFORES,
                "resource_role": "primary_knowledge_source",
            }
        ],
    }


def _valid_node(kg_node: dict) -> dict:
    """A KG node with the non-empty ``categories`` TRAPI 2.0 requires."""
    if not kg_node.get("categories"):
        kg_node = {**kg_node, "categories": list(FALLBACK_CATEGORIES)}
    return kg_node


async def sipr(task, logger: logging.Logger):
    try:
        # given a task, get the message from the db
        logger.info("Getting message from db")
        query_id = task[1]["query_id"]
        response_id = task[1]["response_id"]
        message = await get_message(query_id, logger)

        # check if query is an Set Input Query
        is_set_input_query = False
        for node in message["message"]["query_graph"]["nodes"].values():
            if node.get("set_interpretation") == "MANY":
                is_set_input_query = True
                break

        if not is_set_input_query:
            raise NotImplementedError

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
            },
        }
        for in_node in nodes:
            kg_node = {
                "categories": list(FALLBACK_CATEGORIES),
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
            final_message["message"]["knowledge_graph"]["nodes"][in_node] = _valid_node(
                kg_node
            )
        # TRAPI 2.0: one NodeBinding {"ids": [...]} per qnode.
        input_node_binding = make_binding(nodes)
        for node, score in node_scores:
            if score < 0.001:
                # throw out any nodes with a too low score
                continue
            kg_node = {
                "categories": list(FALLBACK_CATEGORIES),
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
            final_message["message"]["knowledge_graph"]["nodes"][node] = _valid_node(
                kg_node
            )
            new_edge_ids = []
            for in_node in nodes:
                new_edge_id = str(uuid.uuid4())[:8]
                final_message["message"]["knowledge_graph"]["edges"][new_edge_id] = (
                    make_sipr_edge(in_node, node)
                )
                new_edge_ids.append(new_edge_id)
            final_message["message"]["results"].append(
                {
                    "analyses": [
                        {
                            "edge_bindings": {
                                "e0": make_binding(new_edge_ids),
                            },
                            "resource_id": SIPR_INFORES,
                            "score": score,
                        }
                    ],
                    "node_bindings": {
                        "SN": deepcopy(input_node_binding),
                        "ON": make_binding([node]),
                    },
                }
            )

        await save_response(response_id, final_message, logger)

    except NotImplementedError:
        logger.info("SIPR only supports Set Input Queries.")

    task[1]["workflow"] = json.dumps(
        [
            {"id": "sipr"},
            {"id": "sort_results_score"},
        ]
    )


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, sipr)


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, TASK_LIMIT
            ):
                asyncio.create_task(process_task(task, parent_ctx, logger, limiter))
        except asyncio.CancelledError:
            LOGGER.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
