"""SIPR (Set-Input Page Rank) module."""

import asyncio
import json
import logging
import time
import uuid
from copy import deepcopy
from typing import Optional

import httpx
import networkx as nx

from shepherd_utils.config import settings
from shepherd_utils.db import (
    get_message,
    save_response,
)
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.query_errors import QueryError
from shepherd_utils.shared import get_tasks, run_task_lifecycle
from shepherd_utils.task_deadline import get_deadline
from shepherd_utils.trapi import make_binding

# Queue name
STREAM = "sipr"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
#: Ceiling on the time SIPR spends retrieving, for a task carrying no deadline.
MAX_QUERY_TIME = 2400
#: Input curies per retrieval query, and how many of those run at once.
CURIE_BATCH_SIZE = 100
BATCH_CONCURRENCY = 4
#: How many of a hop's best-scoring nodes the next hop expands.
TOP_CANDIDATES = 15
#: Provenance for the edges SIPR infers, matching merge_message's
#: ``infores:shepherd-{target}`` convention.
SIPR_INFORES = "infores:shepherd-sipr"
FALLBACK_CATEGORIES = ["biolink:NamedThing"]
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)


def _seconds_left(deadline: Optional[float]) -> float:
    """Time left for retrieval: up to the query's deadline, else the ceiling."""
    if deadline is None:
        return float(MAX_QUERY_TIME)
    left = min(deadline - time.time(), float(MAX_QUERY_TIME))
    if left <= 0:
        raise QueryError(
            "SIPR could not retrieve the input set's neighborhood within the "
            "query's time budget",
            504,
        )
    return left


async def get_neighborhood(
    id_list: list[str], depth: int, logger, deadline: Optional[float] = None
):
    """Retrieve ``depth`` hops of neighborhood around ``id_list``.

    Each hop queries its curies in batches of ``CURIE_BATCH_SIZE``, at most
    ``BATCH_CONCURRENCY`` at once and within the query's deadline. Every
    batch's nodes are scored by PageRank personalized on that batch's own
    curies, and the next hop expands the ``TOP_CANDIDATES`` best across all
    batches. (Each batch used to replace the candidates outright, so only the
    last batch's nodes were ever expanded.)
    """
    used = set()
    candidate_list = list(dict.fromkeys(id_list))
    trapi_responses = []
    slots = asyncio.Semaphore(BATCH_CONCURRENCY)

    async def _retrieve(batch, hop_num):
        async with slots:
            timeout = _seconds_left(deadline)
            step = write_trapi(batch, hop_num, timeout)
            response = await run_trapi(step, logger, timeout)
        return await asyncio.to_thread(score_nodes, response, batch, logger)

    for hop_num in range(depth):
        query_nodes = [node_id for node_id in candidate_list if node_id not in used]
        if len(query_nodes) == 0:
            break
        used.update(query_nodes)
        batches = [
            query_nodes[i : i + CURIE_BATCH_SIZE]
            for i in range(0, len(query_nodes), CURIE_BATCH_SIZE)
        ]
        outcomes = await asyncio.gather(
            *(_retrieve(batch, hop_num) for batch in batches)
        )
        scores = {}
        for batch_scores, filtered_response in outcomes:
            trapi_responses.append(filtered_response)
            for node_id, score in batch_scores.items():
                scores[node_id] = max(score, scores.get(node_id, 0.0))
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        candidate_list = [node_id for node_id, _ in ranked[:TOP_CANDIDATES]]
    return trapi_responses


def get_nodes(response, query_nodes, logger):
    """The best-scoring neighbors of ``query_nodes`` and the filtered graph."""
    scores, filtered = score_nodes(response, query_nodes, logger)
    return list(scores), filtered


def score_nodes(response, query_nodes, logger):
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
        :TOP_CANDIDATES
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
    return filtered_nodes, final_filtered_graph


def write_trapi(id_list, hop_num, timeout: float = MAX_QUERY_TIME):
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
            "timeout": max(1, int(timeout)),
        },
    }
    return query


async def run_trapi(query, logger, timeout: float = MAX_QUERY_TIME):
    """POST ``query`` to KG retrieval and return its response.

    Raises when retrieval fails. It used to log and hand back the query
    itself, so SIPR saved an empty answer and reported the query OK.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                settings.sync_kg_retrieval_url,
                json=query,
            )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to get a good response from kg retrieval: {str(e)}")
        raise


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
    present = [node for node in target_nodes if node in G]
    if present:
        # Only targets in the graph: an all-zero vector (none of them present)
        # makes pagerank fail to normalize, so that case runs unpersonalized.
        personalization = {node: 0.0 for node in G.nodes()}
        for target_node_id in present:
            personalization[target_node_id] = 1.0 / len(present)

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


def query_shape(query_graph: dict) -> tuple[str, str, str]:
    """The set node, answer node and edge of a SIPR query graph.

    SIPR answers one-hop queries from a set of input ids
    (``set_interpretation: MANY``) to an answer node; anything else is a query
    it doesn't do (501). The ids are read from the query graph, whatever the
    client named its qnodes -- not from fixed ``SN`` / ``ON`` / ``e0`` keys.
    """
    nodes = query_graph.get("nodes") or {}
    edges = query_graph.get("edges") or {}
    set_nodes = [
        qnode_id
        for qnode_id, qnode in nodes.items()
        if (qnode or {}).get("set_interpretation") == "MANY"
    ]
    if len(set_nodes) != 1 or len(edges) != 1 or len(nodes) != 2:
        raise QueryError(
            "SIPR only answers one-hop queries from a single input set "
            "(set_interpretation MANY) to one answer node",
            501,
        )
    set_node = set_nodes[0]
    edge_id, edge = next(iter(edges.items()))
    ends = {edge.get("subject"), edge.get("object")}
    answer_node = next(qnode_id for qnode_id in nodes if qnode_id != set_node)
    if ends != {set_node, answer_node}:
        raise QueryError(
            "SIPR's query edge must connect the input set to the answer node",
            400,
        )
    if not nodes[set_node].get("ids"):
        raise QueryError("SIPR's input set node has no ids", 400)
    return set_node, answer_node, edge_id


async def sipr(task, logger: logging.Logger):
    # given a task, get the message from the db
    logger.info("Getting message from db")
    query_id = task[1]["query_id"]
    response_id = task[1]["response_id"]
    message = await get_message(query_id, logger)
    query_graph = message["message"]["query_graph"]

    # Only set-input queries; a QueryError ends the query with its code
    # instead of the empty OK answer this used to save.
    set_node, answer_node, edge_id = query_shape(query_graph)

    # graph retrieval
    nodes = list(query_graph["nodes"][set_node]["ids"])
    logger.info(f"Getting neighborhood for {nodes}")
    trapi_responses = await get_neighborhood(
        nodes, 2, logger, deadline=get_deadline(task[1])
    )

    # distribute weights: CPU-bound, so off the event loop
    logger.info("Distributing weights")
    ppr = await asyncio.to_thread(distribute_weights, trapi_responses, nodes, logger)
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
                kg_node = trapi_response["message"]["knowledge_graph"]["nodes"][in_node]
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
                kg_node = trapi_response["message"]["knowledge_graph"]["nodes"][node]
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
                            edge_id: make_binding(new_edge_ids),
                        },
                        "resource_id": SIPR_INFORES,
                        "score": score,
                    }
                ],
                "node_bindings": {
                    set_node: deepcopy(input_node_binding),
                    answer_node: make_binding([node]),
                },
            }
        )

    await save_response(response_id, final_message, logger)

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
