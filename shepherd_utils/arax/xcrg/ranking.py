# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/ranking.py (DEC-21).
# Changes from upstream:
#   - TRAPI 2.0: knowledge_level/agent_type are read from the Edge members (get_qualified_stmt takes the Edge); a value without a weight counts as unknown
#   - TRAPI 2.0: one NodeBinding per qnode (ids); optional KG edges/attributes read through *_dict/*_list
# See README.md.
from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, cast

from translator_tom import (
    Attribute,
    CURIE,
    Edge,
    Message,
    QNodeID,
    QueryGraph,
    Response,
)

from . import DebugLevel, biolink, trapi
from .biolink import Agent_Type, Knowledge_Level
from .context import RunContext
from .ngd import get_ngd_score
from .utilities import XCRGResult, as_type


@dataclass(frozen = True, slots = False)
class QualifiedStatement:
    num_publications : int
    num_studies      : int
    evidence_count   : int
    knowledge_level  : str          # KnowledgeLevelEnum
    agent_type       : str | None   # AgentTypeEnum
    confidence_score : float | None


@dataclass(slots = False)
class Result_Summary:
    result                 : XCRGResult # The original result
    curie                  : CURIE
    name                   : str
    score                  : float # The final score of the result
    specificity            : int
    information_content    : int
    num_direct_edges       : int
    direct_qualified_stmts : list[QualifiedStatement]
    num_xcrg_nodes         : int
    num_xcrg_edges         : int
    xcrg_qualified_stmts   : list[QualifiedStatement]


class Evidence_Category(Enum):
    NUM_STUDIES      = "num_studies"
    NUM_PUBLICATIONS = "num_publications"
    EVIDENCE_COUNT   = "evidence_count"


@dataclass
class Count_Category:
    log_function : Callable[[float], float]
    factor       : float


@dataclass(frozen = True)
class Scoring_Params:
    k                       : float
    agent_type_weights      : dict[biolink.Agent_Type, float]
    knowledge_level_weights : dict[biolink.Knowledge_Level, float]
    confidence_factor       : float
    category_weights        : dict[Evidence_Category, Count_Category]
    direct_evidence_weight  : float
    ngd_score_factor        : float


# These values are mostly determined using scripts/run_optimizer
DEFAULT_SCORING_PARAMS = Scoring_Params(
    k = 60, # Manually set; much literature argues that 60 is a good default for RRF
    agent_type_weights = {
        Agent_Type.MANUAL_AGENT: 57,
        Agent_Type.AUTOMATED_AGENT: 85,
        Agent_Type.COMPUTATIONAL_MODEL: 45,
        Agent_Type.TEXT_MINING_AGENT: 61,
    },
    knowledge_level_weights = {
        Knowledge_Level.KNOWLEDGE_ASSERTION: 97,
        Knowledge_Level.LOGICAL_ENTAILMENT: 25,
        Knowledge_Level.PREDICTION: 68,
        Knowledge_Level.STATISTICAL_ASSOCIATION: 11,
        Knowledge_Level.TEXT_CO_OCCURRENCE: 3,
        Knowledge_Level.OBSERVATION: 15,
        Knowledge_Level.NOT_PROVIDED: 38,
    },
    confidence_factor = 41,
    category_weights = {
        Evidence_Category.NUM_STUDIES: Count_Category(
            log_function = lambda x: x,
            factor = 97
         ),
        Evidence_Category.NUM_PUBLICATIONS: Count_Category(
            log_function = lambda x: x,
            factor = 61
         ),
        Evidence_Category.EVIDENCE_COUNT: Count_Category(
            log_function = math.log10,
            factor = 76
         ),
    },
    direct_evidence_weight = 97,
    ngd_score_factor = 10000 # Manually set
)


def get_qualified_stmt(edge: Edge) -> QualifiedStatement:
    num_publications = 0
    num_studies = 0
    evidence_count = 0
    # TRAPI 2.0 edges always carry both; a value the weights don't cover (e.g. agent_type
    # not_provided) counts as unknown, as a missing 1.x attribute did
    knowledge_level: str = (
        edge.knowledge_level
        if edge.knowledge_level in Knowledge_Level._value2member_map_
        else "not_provided"
    )
    agent_type: str | None = (
        edge.agent_type if edge.agent_type in Agent_Type._value2member_map_ else None
    )
    confidence_score: float | None = None

    for attribute in edge.attributes_list:
        match attribute.attribute_type_id:
            case "biolink:publications":
                if publications := as_type(attribute.value, list):
                    num_publications = len(publications)
            case "biolink:has_supporting_studies":
                # TODO: Does this attribute value have a canonical structure defined somewhere?
                if obj := as_type(attribute.value, dict):
                    for key in obj:
                        num_studies = len(obj[key].get("has_study_results") or [])
            case "biolink:evidence_count":
                evidence_count = as_type(attribute.value, int) or 0
            case "biolink:has_confidence_score":
                confidence_score = as_type(attribute.value, float)

    return QualifiedStatement(
        num_publications = num_publications,
        num_studies = num_studies,
        evidence_count = evidence_count,
        knowledge_level = knowledge_level,
        agent_type = agent_type,
        confidence_score = confidence_score
    )


def create_result_summary(
    ctx: RunContext,
    message: Message,
    result: XCRGResult,
    answer_qid: QNodeID,
    use_category_specificity: bool,
) -> Result_Summary:
    """Return sort metrics for the answer node bound by a result."""
    assert (kgraph := message.knowledge_graph)

    if not (answer_id := trapi.get_bound_node_curie(result, answer_qid)):
        raise Exception(f"Failed to find CURIE for answer QNodeID: {answer_qid}")
    answer_node = kgraph.nodes[answer_id]
    kg_edges = kgraph.edges_dict
    answer_name = answer_node.name or "" # TODO

    specificity = (
        biolink.get_node_category_specificity(ctx, answer_node)
        if use_category_specificity
        else 0
    )

    attributes: list[Attribute] = []
    if answer_node:
        attributes = answer_node.attributes_list

    values: list[int] = [0]
    for attribute in attributes:
        if attribute.attribute_type_id != "biolink:information_content":
            continue
        raw_value = attribute.value
        raw_values = raw_value if isinstance(raw_value, list) else [raw_value]
        for value in raw_values:
            try:
                values.append(int(value)) # TODO
            except (TypeError, ValueError):
                continue
    information_content: int = max(values)

    num_direct_edges = len(result.xcrg_direct_binding_ids)

    xcrg_nodes = set[CURIE]()
    num_xcrg_edges = len(result.xcrg_support_edge_ids)
    for binding in result.xcrg_direct_binding_ids:
        edge = kg_edges[binding]
        xcrg_nodes.add(edge.subject)
        xcrg_nodes.add(edge.object)
    num_xcrg_nodes = len(xcrg_nodes)

    direct_qualified_stmts = list[QualifiedStatement]()
    for edge_id in result.xcrg_direct_binding_ids:
        statement = get_qualified_stmt(kg_edges[edge_id])
        direct_qualified_stmts.append(statement)

    xcrg_qualified_stmts = list[QualifiedStatement]()
    for edge_id in result.xcrg_support_edge_ids:
        statement = get_qualified_stmt(kg_edges[edge_id])
        xcrg_qualified_stmts.append(statement)

    result.ngd_score = get_ngd_score(
        ctx,
        result.node_bindings[ctx.subject_qid].ids[0],
        result.node_bindings[ctx.object_qid].ids[0]
    )

    return Result_Summary(
        result = result,
        curie = answer_id,
        name = answer_name,
        score = 0,
        specificity = specificity,
        information_content = information_content,
        num_direct_edges = num_direct_edges,
        direct_qualified_stmts = direct_qualified_stmts,
        num_xcrg_nodes = num_xcrg_nodes,
        num_xcrg_edges = num_xcrg_edges,
        xcrg_qualified_stmts = xcrg_qualified_stmts,
    )


@dataclass
class Ranker(ABC):
    """Base class for xCRG ranker."""
    scoring_params: Scoring_Params = field(default = DEFAULT_SCORING_PARAMS)

    @abstractmethod
    def rank_results(self, summaries: dict[CURIE, Result_Summary]) -> list[Result_Summary]:
        """Score, sort, and rank results in the response."""
        ...


class Custom_Ranker(Ranker):
    """Ranker that implements a custom strategy."""

    def score_qualified_stmt(self, stmt: QualifiedStatement) -> float:
        score: float = 0

        agent_factor = 1
        if stmt.agent_type:
            agent_type = biolink.Agent_Type(stmt.agent_type)
            agent_factor = self.scoring_params.agent_type_weights[agent_type]

        knowledge_factor = 1
        if stmt.knowledge_level:
            knowledge_level = biolink.Knowledge_Level(stmt.knowledge_level)
            knowledge_factor = self.scoring_params.knowledge_level_weights[knowledge_level]

        # Try and normalize confidence score; these can vary wildly
        confidence_factor: float = 1
        match stmt.confidence_score or 0:
            case x if 0 < x < 1:   confidence_factor = 1 + x
            case x if 0 < x < 10:  confidence_factor = 1 + (x / 10)
            case x if 0 < x < 100: confidence_factor = 1 + (x / 100)

        factors = [
            agent_factor,
            knowledge_factor,
            confidence_factor
        ]

        category = self.scoring_params.category_weights[Evidence_Category.NUM_STUDIES]
        score += category.log_function(stmt.num_studies + 1) * category.factor

        category = self.scoring_params.category_weights[Evidence_Category.NUM_PUBLICATIONS]
        score += category.log_function(stmt.num_publications + 1) * category.factor

        category = self.scoring_params.category_weights[Evidence_Category.EVIDENCE_COUNT]
        score += category.log_function(stmt.evidence_count + 1) * category.factor

        score *= (sum(factors) / len(factors))

        return score

    def calculate_score_for_result(self, summary: Result_Summary) -> float:
        total_score: float = 0

        for stmt in summary.direct_qualified_stmts:
            total_score += self.score_qualified_stmt(stmt) * self.scoring_params.direct_evidence_weight

        for stmt in summary.xcrg_qualified_stmts:
            total_score += self.score_qualified_stmt(stmt)

        # TODO: information_content
        # TODO: specificity

        # Reward results with an NGD score
        if ngd_score := summary.result.ngd_score:
            total_score += ngd_score * self.scoring_params.ngd_score_factor

        # Penalize results with no direct connections
        if not summary.direct_qualified_stmts:
            total_score *= 0.5

        return total_score

    def rank_results(self, summaries: dict[CURIE, Result_Summary]) -> list[Result_Summary]:
        summaries: list[Result_Summary] = list(summaries.values())
        for summary in summaries:
            summary.score = self.calculate_score_for_result(summary)
        return sorted(summaries, key = lambda x: x.score, reverse = True)


@dataclass
class RRF_Ranker(Ranker):
    """Ranker that implements Reciprocal Rank Fusion (RRF) strategy."""
    @staticmethod
    def rank_summaries(
        results: dict[CURIE, Result_Summary],
        ranks: dict[CURIE, list[float]],
        mapper: Callable[[Result_Summary], float]
    ):
        stats = sorted(results.values(), key = mapper, reverse = True)
        # We need to tie-break rankings for items with the same score
        i = 0
        n = len(stats)
        while i < n:
            j = i
            # find the extent of the tie block
            while j < n and mapper(stats[i]) == mapper(stats[j]):
                j += 1
            avg_rank = (i + 1 + j) / 2.0  # average of positions i+1..j
            for idx in range(i, j):
                ranks.setdefault(stats[idx].curie, []).append(avg_rank)
            i = j
        return ranks

    def score_stmts(self, statements: list[QualifiedStatement], category: Evidence_Category):
        total_score: float = 0

        for stmt in statements:
            score = 0

            agent_factor = 1
            if stmt.agent_type:
                agent_type = biolink.Agent_Type(stmt.agent_type)
                agent_factor = self.scoring_params.agent_type_weights[agent_type]

            knowledge_factor = 1
            if stmt.knowledge_level:
                knowledge_level = biolink.Knowledge_Level(stmt.knowledge_level)
                knowledge_factor = self.scoring_params.knowledge_level_weights[knowledge_level]

            # Try and normalize confidence score; these can vary wildly
            confidence_factor: float = 1
            match stmt.confidence_score or 0:
                case x if 0 < x < 1:   confidence_factor = 1 + x
                case x if 0 < x < 10:  confidence_factor = 1 + (x / 10)
                case x if 0 < x < 100: confidence_factor = 1 + (x / 100)

            category_factor = self.scoring_params.category_weights[category].factor

            match category:
                case Evidence_Category.NUM_PUBLICATIONS: score += stmt.num_publications + 1
                case Evidence_Category.NUM_STUDIES:      score += stmt.num_studies      + 1
                case Evidence_Category.EVIDENCE_COUNT:   score += stmt.evidence_count   + 1

            factors = [
                agent_factor,
                knowledge_factor,
                confidence_factor,
                category_factor
            ]

            score *= (sum(factors) / len(factors))

            total_score += score

        return total_score

    def rank_results(self, summaries: dict[CURIE, Result_Summary]) -> list[Result_Summary]:
        ranks = dict[CURIE, list[float]]()

        ranker = Custom_Ranker(scoring_params = self.scoring_params)

        self.rank_summaries(summaries, ranks, lambda x: x.specificity)
        self.rank_summaries(summaries, ranks, lambda x: x.information_content)
        self.rank_summaries(summaries, ranks, lambda x: x.num_direct_edges)
        self.rank_summaries(summaries, ranks, lambda x: x.num_xcrg_nodes)
        self.rank_summaries(summaries, ranks, lambda x: x.num_xcrg_edges)
        self.rank_summaries(summaries, ranks, lambda x: x.result.ngd_score or 0)
        self.rank_summaries(summaries, ranks, lambda x: ranker.calculate_score_for_result(x))
        self.rank_summaries(summaries, ranks, lambda x: self.score_stmts(x.direct_qualified_stmts, Evidence_Category.NUM_PUBLICATIONS))
        self.rank_summaries(summaries, ranks, lambda x: self.score_stmts(x.direct_qualified_stmts, Evidence_Category.NUM_STUDIES))
        self.rank_summaries(summaries, ranks, lambda x: self.score_stmts(x.direct_qualified_stmts, Evidence_Category.EVIDENCE_COUNT))
        self.rank_summaries(summaries, ranks, lambda x: self.score_stmts(x.xcrg_qualified_stmts, Evidence_Category.NUM_PUBLICATIONS))
        self.rank_summaries(summaries, ranks, lambda x: self.score_stmts(x.xcrg_qualified_stmts, Evidence_Category.NUM_STUDIES))
        self.rank_summaries(summaries, ranks, lambda x: self.score_stmts(x.xcrg_qualified_stmts, Evidence_Category.EVIDENCE_COUNT))

        k = self.scoring_params.k

        for curie, summary in summaries.items():
            for rank in ranks[curie]:
                summary.score += 1.0 / (rank + k)

        return sorted(summaries.values(), key = lambda x: x.score, reverse = True)


def rank_results(ctx: RunContext, response: Response, results: list[XCRGResult]) -> list[XCRGResult]:
    """Score, sort, rank, and limit results in the response."""
    qgraph = cast(QueryGraph, response.message.query_graph)

    answer_qid = ctx.get_answer_qid()

    use_category_specificity = False
    if qnode := qgraph.nodes.get(answer_qid):
        use_category_specificity = any(biolink.is_chemical_category(ctx, c) for c in qnode.categories_list)

    summaries = dict[CURIE, Result_Summary]()
    for result in results:
        summary = create_result_summary(
            ctx,
            response.message,
            result,
            answer_qid,
            use_category_specificity
        )
        summaries[summary.curie] = summary

    ranker = Custom_Ranker()
    # ranker = RRF_Ranker()

    ranked_summaries = ranker.rank_results(summaries)

    ctx.debug_dump_json(
        label = "xcrg_scored_results",
        payload = [
            {
                "rank": i,
                "curie": x.curie,
                "name": x.name,
                "score": x.score,
                "specificity": x.specificity,
                "information_content": x.information_content,
                "num_direct_edges": x.num_direct_edges,
                "direct_qualified_stmts": x.direct_qualified_stmts,
                "num_xcrg_nodes": x.num_xcrg_nodes,
                "num_xcrg_edges": x.num_xcrg_edges,
                "xcrg_qualified_stmts": x.xcrg_qualified_stmts,
                "ngd_score": x.result.ngd_score,
            }
            for i, x in enumerate(ranked_summaries, start = 1)
        ],
        level = DebugLevel.BASIC
    )

    final_results = [x.result for x in ranked_summaries]
    final_results = final_results[0:ctx.num_max_results]

    return final_results
