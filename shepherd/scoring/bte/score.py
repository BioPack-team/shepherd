import collections
from datetime import datetime
from typing import Any, Union, cast, Optional
import httpx
import asyncio
import os
import math
import statistics
import time

from pydantic import BaseModel, parse_obj_as
import pydantic
from reasoner_pydantic import (
    Attribute,
    AuxiliaryGraphs,
    Edge,
    HashableSequence,
    KnowledgeGraph,
    HashableSequence,
    KnowledgeGraph,
    LogEntry,
    LogLevel,
    Node,
    Edge,
    QueryGraph,
    Response,
    Result,
    Results,
)
from reasoner_pydantic.shared import LogLevelEnum

# CONFIGURABLE
NGD_BATCH_SIZE = 1000
NGD_RATE_LIMIT = 300 * 30  # semmed theoretically has 300/s limit, use half to be safe

EDGE_WEIGHT = 1.0
TEXT_MINED_EDGE_WEIGHT = 0.5
NGD_WEIGHT = 0.25
LENGTH_PENALTY = 2.0
TUNING_PARAM = 2.0

NGD_URLS = {
    "development": "https://biothings.ci.transltr.io/semmeddb/query/ngd",
    "staging": "https://biothings.ci.transltr.io/semmeddb/query/ngd",
    "testing": "https://biothings.test.transltr.io/semmeddb/query/ngd",
    "production": "https://biothings.ncats.io/semmeddb/query/ngd",
}

# CALCULATED FROM CONFIGURABLE
NGD_SEMAPHORE = asyncio.Semaphore(NGD_RATE_LIMIT)


# sigmoid function scaled from 0 to 1
def scaled_sigmoid(input: float) -> float:
    tuned_input = max(input, 0) / TUNING_PARAM
    sigmoid = 1 / (1 + math.exp(-tuned_input))
    return sigmoid * 2 - 1


def inverse_scaled_sigmoid(input: float) -> float:
    return -TUNING_PARAM * math.log(2 / (input + 1) - 1)


def is_xref(attribute: Attribute) -> bool:
    return attribute.attribute_type_id == "biolink:xref"


def get_umls(node: Node) -> list[str]:
    xref: Attribute | None = next(filter(is_xref, node.attributes))
    if xref is None:
        return []
    umls = list(
        set(
            curie.removeprefix("UMLS:")
            for curie in cast(str, xref.value)
            if "UMLS:" in curie
        )
    )

    # If more than 25, take roughly even-spaced sample of 25
    if len(umls) > 25:
        umls = umls[:: math.floor(len(umls) / 25)][:25]
    return umls


def get_pairs(
    knowledge_graph: KnowledgeGraph,
) -> tuple[list[list[str]], dict[str, set[str]]]:
    umls_pairs: dict[str, set[str]] = {}
    umls_pairs_flat: list[list[str]] = []
    edge_ids_by_pair: dict[str, set[str]] = {}

    for edge_id, edge in knowledge_graph.edges.items():
        edge = cast(Edge, edge)
        subject_umls = get_umls(knowledge_graph.nodes[edge.subject])
        object_umls = get_umls(knowledge_graph.nodes[edge.object])

        for subject_curie in subject_umls:
            if subject_curie not in umls_pairs:
                umls_pairs[subject_curie] = set()
            for object_curie in object_umls:
                if object_curie in umls_pairs[subject_curie]:
                    continue

                umls_pairs[subject_curie].add(object_curie)
                umls_pairs_flat.append([subject_curie, object_curie])
                pair_str = f"{subject_curie}-{object_curie}"
                if pair_str not in edge_ids_by_pair:
                    edge_ids_by_pair[pair_str] = set()
                edge_ids_by_pair[pair_str].add(edge_id)
    return umls_pairs_flat, edge_ids_by_pair


async def query_limited(batch):
    async with NGD_SEMAPHORE, httpx.AsyncClient() as client:
        try:
            response = await client.post(
                NGD_URLS[os.environ.get("SERVER_MATURITY", "dev")],
                json={"umls": batch, "expand": "both"},
                timeout=httpx.Timeout(5, read=None),
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as err:
            print(f"NGD request fails due to {err}")
            return []


async def query_ngd(umls_pairs: list[list[str]]) -> dict[str, float]:
    start = time.time()
    batches = [
        umls_pairs[i : i + NGD_BATCH_SIZE]
        for i in range(0, len(umls_pairs), NGD_BATCH_SIZE)
    ]

    class NGDScore(BaseModel):
        ngd: Union[float, str]
        reason: Optional[str]
        umls: list[str]

    responses = await asyncio.gather(*[query_limited(batch) for batch in batches])
    raw_scores: list[NGDScore] = []
    for response in responses:
        try:
            raw_scores.extend(parse_obj_as(list[NGDScore], response))
        except pydantic.ValidationError as err:
            print(f"NGD response failed validation due to {err}")
            continue

    scores = {
        f"{score.umls[0]}-{score.umls[1]}": score.ngd / 1
        for score in raw_scores
        if (isinstance(score.ngd, float) and not math.isinf(score.ngd))
    }
    end = time.time()
    print(f"NGD lookup took a total of {end - start}s for {len(batches)} queries.")
    print(f"Retrieved {len(scores.keys())} NGD scores.")

    return scores


def is_text_mined(edge: Edge) -> bool:
    agent_type = next(
        iter(
            attr
            for attr in (edge.attributes or [])
            if "agent_type" in attr.attribute_type_id
            or "agent_type" in (attr.original_attribute_name or "")
        ),
        None,
    )
    if agent_type is not None:
        return agent_type.value == "text_mining_agent"
    return False


def consolidate_edges_from_result(
    result: Result,
    knowledge_graph: KnowledgeGraph,
    auxiliary_graphs: AuxiliaryGraphs | None,
) -> dict[str, Edge]:

    result_bound_edges: dict[str, Edge] = {}
    for bound_edges in next(iter(result.analyses)).edge_bindings.values():
        result_bound_edges.update(
            {edge.id: knowledge_graph.edges[edge.id] for edge in bound_edges}
        )

    supporting_edges: dict[str, Edge] = {}
    for edge in result_bound_edges.values():
        support_graphs = next(
            (
                cast(list[str], attr.value)
                for attr in (edge.attributes or [])
                if attr.attribute_type_id == "biolink:support_graphs"
            ),
            [],
        )
        for graph_id in support_graphs:
            edges = {
                cast(str, edge_id): knowledge_graph.edges[edge_id]
                for edge_id in cast(AuxiliaryGraphs, auxiliary_graphs)[graph_id].edges
            }
            supporting_edges.update(edges)

    result_bound_edges.update(supporting_edges)

    return result_bound_edges


def calculate_score(
    result_edges: dict[str, Edge], ngd_by_edge: dict[str, float]
) -> tuple[float, bool]:
    score = 0
    scored_by_ngd = False
    edge_scores: dict[str, float] = {}

    class NodeDegree(BaseModel):
        incoming: int = 0
        outgoing: int = 0

    node_degrees: dict[str, NodeDegree] = {}
    edges_starting_from_node: dict[str, list[str]] = {}

    for edge_id, edge in result_edges.items():
        # Keep track of indegrees and outdegrees to find start and end nodes later
        if edge.subject in node_degrees:
            node_degrees[edge.subject].outgoing += 1
        else:
            node_degrees[edge.subject] = NodeDegree(outgoing=1)
        if edge.object in node_degrees:
            node_degrees[edge.object].incoming += 1
        else:
            node_degrees[edge.object] = NodeDegree(incoming=1)

        # Track edge connections to find paths
        if edge.subject in edges_starting_from_node:
            edges_starting_from_node[edge.subject].append(edge_id)
        else:
            edges_starting_from_node[edge.subject] = [edge_id]

        edge_score = (
            TEXT_MINED_EDGE_WEIGHT if is_text_mined(edge) is True else EDGE_WEIGHT
        )
        ngd_score = ngd_by_edge.get(edge_id, 0)
        if ngd_score > 0:
            scored_by_ngd = True

        edge_scores[edge_id] = NGD_WEIGHT * ngd_score + edge_score

    # Perform BFS to find paths
    start_node = next(
        node for node in node_degrees.keys() if node_degrees[node].incoming == 0
    )
    end_node = next(
        node for node in node_degrees.keys() if node_degrees[node].outgoing == 0
    )

    # Better optimized for queue than list
    queue: collections.deque[tuple[str, float, int]] = collections.deque(
        [(start_node, 0.0, 0)]
    )

    while len(queue) > 0:
        node: str
        path_score: float
        path_len: int
        node, path_score, path_len = queue.popleft()
        if node == end_node:
            score += path_score / math.pow(path_len, LENGTH_PENALTY)
        elif node in edges_starting_from_node:
            for edge_idx in edges_starting_from_node[node]:
                queue.append(
                    (
                        result_edges[edge_idx].object,
                        path_score + edge_scores[edge_idx],
                        path_len + 1,
                    )
                )

    return scaled_sigmoid(score), scored_by_ngd


async def get_ngd_scores(
    umls_pairs: list[list[str]], edge_ids_by_pair: dict[str, set[str]]
):
    ngd_by_edge: dict[str, Any] = {}
    try:
        async with asyncio.timeout(10):
            pair_scores = await query_ngd(umls_pairs)
        # Get average score per edge
        for pair, score in pair_scores.items():
            for edge_id in edge_ids_by_pair[pair]:
                if edge_id not in ngd_by_edge:
                    ngd_by_edge[edge_id] = []
                ngd_by_edge[edge_id].append(score)
        for edge_id, scores in ngd_by_edge.items():
            ngd_by_edge[edge_id] = statistics.fmean(scores)
    except TimeoutError:
        print("NGD scoring skipped due to timeout (took >5s).")
        pass

    return ngd_by_edge


async def score_bte(response_body: dict[str, Any]) -> dict[str, Any]:
    if response_body.get("message", None) is None:
        return {}
    response = parse_obj_as(Response, response_body)

    # Assume all edges are used in a result (guaranteed by Retriever)
    knowledge_graph = cast(KnowledgeGraph, response.message.knowledge_graph)
    umls_pairs, edge_ids_by_pair = get_pairs(knowledge_graph)
    ngd_by_edge = await get_ngd_scores(umls_pairs, edge_ids_by_pair)

    scored_by_ngd_count = 0
    result_count = 0
    for result in cast(Results, response.message.results):
        result_count += 1
        consolidated_edges = consolidate_edges_from_result(
            result, knowledge_graph, response.message.auxiliary_graphs
        )
        score, scored_by_ngd = calculate_score(consolidated_edges, ngd_by_edge)
        if scored_by_ngd:
            scored_by_ngd_count += 1
        next(iter(result.analyses)).score = score

    log_message = " ".join(
        [
            f"Scored {result_count} results",
            f"({scored_by_ngd_count} with /",
            f"{result_count - scored_by_ngd_count} without SEMMEDDB NGD).",
        ]
    )

    cast(HashableSequence[LogEntry], response.logs).append(
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel(__root__=LogLevelEnum.debug),
            code=None,
            message=log_message,
        )
    )

    return response.dict(exclude_none=True)
