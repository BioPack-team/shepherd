# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/queries.py (DEC-21).
# Changes from upstream:
#   - TRAPI 2.0: qualifier sets go in constraints.qualifiers ({type_id: value}); no empty auxiliary_graphs
#   - lookups send parameters (the query's, with timeout and tiers defaulted from the config; upstream's 2.0 TODO)
# See README.md.
from __future__ import annotations

from copy import deepcopy

from translator_tom import (
    CURIE,
    KnowledgeGraph,
    Message,
    QEdge,
    QEdgeConstraints,
    QNode,
    Query,
    QueryGraph,
    QueryParameters,
)

from .constants import TF_QNODE_ID, DIRECT_QEDGE_ID
from .context import RunContext
from .models import Direction


def build_lookup_parameters(ctx: RunContext) -> QueryParameters:
    """Retriever lookup parameters: the original query's, with xCRG's timeout and data tiers as defaults."""
    parameters = deepcopy(ctx.query.parameters) if ctx.query.parameters else QueryParameters()
    parameters.timeout = parameters.timeout or ctx.timeout
    parameters.extra_set("tiers", parameters.extra_get("tiers") or list(ctx.config.tiers))
    return parameters


def build_two_hop_query(
    ctx: RunContext,
    tf_list: list[CURIE],
    first_direction: Direction,
    second_direction: Direction
) -> Query:
    """Build a TF-mediated two-hop TRAPI query from the original inferred query."""
    return Query(
        message = Message(
            query_graph = QueryGraph(
                nodes = {
                    ctx.subject_qid: deepcopy(ctx.subject_qnode),
                    TF_QNODE_ID: QNode(ids = tf_list, categories = ["biolink:Gene"]),
                    ctx.object_qid: deepcopy(ctx.object_qnode),
                },
                edges = {
                    "e0": QEdge(
                        subject = ctx.subject_qid,
                        object = TF_QNODE_ID,
                        predicates = ["biolink:affects"],
                        constraints = QEdgeConstraints(
                            qualifiers = [
                                {
                                    "biolink:object_aspect_qualifier": "activity_or_abundance",
                                    "biolink:object_direction_qualifier": first_direction.value,
                                }
                            ]
                        ),
                    ),
                    "e1": QEdge (
                        subject = TF_QNODE_ID,
                        object = ctx.object_qid,
                        predicates = ["biolink:affects"],
                        constraints = QEdgeConstraints(
                            qualifiers = [
                                {
                                    "biolink:object_aspect_qualifier": "activity_or_abundance",
                                    "biolink:object_direction_qualifier": second_direction.value,
                                }
                            ]
                        ),
                    ),
                },
            ),
            knowledge_graph = KnowledgeGraph.new(),
            results = [],
        ),
        parameters = build_lookup_parameters(ctx),
        submitter = ctx.query.submitter or ctx.config.resource_id
    )


def build_one_hop_query(ctx: RunContext) -> Query:
    """Build the direct one-hop query that accompanies inferred xCRG mode."""
    direct_edge = deepcopy(ctx.query_edge)
    direct_edge.knowledge_type = None

    return Query(
        message = Message(
            query_graph = QueryGraph(
                nodes = {
                    ctx.subject_qid: deepcopy(ctx.subject_qnode),
                    ctx.object_qid: deepcopy(ctx.object_qnode),
                },
                edges = {
                    DIRECT_QEDGE_ID: direct_edge
                }
            ),
            knowledge_graph = KnowledgeGraph.new(),
            results = [],
        ),
        parameters = build_lookup_parameters(ctx),
        submitter = ctx.query.submitter
    )
