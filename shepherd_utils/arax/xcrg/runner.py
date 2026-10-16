# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/runner.py (DEC-21).
# Changes from upstream:
#   - TRAPI 2.0: qualifiers from constraints.qualifiers; one Node/EdgeBinding per qnode/qedge (ids); the inferred edge's knowledge_level/agent_type are Edge members; aux graphs have no attributes; no empty qualifiers/auxiliary_graphs; no Pathfinder query graph classes
#   - a query dict is read for its query graph, parameters and submitter only (query_from_dict)
#   - the response is dumped in python mode, so NaN attribute values are kept (JSON mode makes them null)
# See README.md.
"""Reusable xCRG direct and inferred lookup logic."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from copy import deepcopy
from typing import Coroutine, cast

from opentelemetry import trace
from translator_tom import (
    CURIE,
    Analysis,
    Attribute,
    AuxGraphID,
    AuxiliaryGraph,
    AuxiliaryGraphsDict,
    Edge,
    EdgeBinding,
    EdgeID,
    FastJsonValue,
    KnowledgeGraph,
    KnowledgeType,
    Message,
    Node,
    NodeBinding,
    QEdge,
    QEdgeID,
    QNode,
    QNodeID,
    Qualifier,
    Query,
    QueryGraph,
    Response,
    Result,
    RetrievalSource,
)

from . import DebugLevel, biolink, ngd, ranking, retriever, trapi
from .constants import TF_QNODE_ID, DIRECT_QEDGE_ID
from .config import XCRGConfig
from .context import RunContext
from .models import (
    DIRECTION_TEMPLATES,
    Batch_Summary,
    Debug_Summary,
    Direction_Template,
    Message_Statistics,
    Template_Summary,
)
from .queries import (
    Direction,
    build_one_hop_query,
    build_two_hop_query
)
from .reporting import LogReporter, Reporter, StubReporter
from .utilities import (
    chunk_values,
    make_stable_id,
    XCRGResult
)


# Defer to the parent application to instantiate global opentelemetry provider.
# We can put tracer in a separate module if we wind up using it elsewhere.
tracer = trace.get_tracer("xcrg")


def build_combined_query_graph(ctx: RunContext) -> QueryGraph:
    """Build a response query graph that can bind direct and TF-mediated results."""
    query = build_one_hop_query(ctx)
    qgraph = cast(QueryGraph, query.message.query_graph)
    qgraph.nodes[TF_QNODE_ID] = QNode(
        ids = ctx.tf_list,
        categories = ["biolink:Gene"]
    )
    qgraph.edges["e0"] = QEdge(
        subject = ctx.subject_qid,
        predicates = ["biolink:affects"],
        object = TF_QNODE_ID
    )
    qgraph.edges["e1"] = QEdge(
        subject = TF_QNODE_ID,
        predicates = ["biolink:affects"],
        object = ctx.object_qid
    )
    return qgraph


def merge_filtered_responses(
    ctx: RunContext,
    responses: list[Response],
    query_graph: QueryGraph
) -> Response:
    """Merge filtered Retriever responses into a single TRAPI response."""
    nodes = dict[CURIE, Node]()
    edges = dict[EdgeID, Edge]()
    aux_graph = AuxiliaryGraphsDict()
    seen_results = set()
    results = list[Result]()

    for response in responses:
        message = response.message

        if kg_graph := message.knowledge_graph:
            merge_retriever_nodes(nodes, kg_graph.nodes)
            merge_retriever_edges(edges, kg_graph.edges_dict)

        aux_graph.update(message.auxiliary_graphs_dict)

        for result in message.results_list:
            # TODO: Is there a better way to do this?
            result_dict = result.to_dict()
            key = json.dumps(
                {
                    "node_bindings": result_dict.get('node_bindings', {}),
                    "analyses": result_dict.get('analyses', [])
                },
                sort_keys=True,
            )

            if key not in seen_results:
                seen_results.add(key)
                results.append(result)

    return Response(
        schema_version = responses[0].schema_version if responses else ctx.trapi_schema_version,
        biolink_version = responses[0].biolink_version if responses else ctx.biolink_version,
        message = Message(
            query_graph = query_graph,
            knowledge_graph = KnowledgeGraph(nodes = nodes, edges = edges),
            results = results,
            auxiliary_graphs = aux_graph or None
        )
    )


def merge_retriever_nodes(merged_nodes: dict[CURIE, Node], incoming_nodes: dict[CURIE, Node]) -> None:
    """Merge Retriever KG nodes without letting sparse duplicates erase metadata."""
    for node_id, incoming_node in incoming_nodes.items():
        existing_node = merged_nodes.get(node_id)
        if existing_node is None or metadata_weight(incoming_node) > metadata_weight(existing_node):
            merged_nodes[node_id] = deepcopy(incoming_node)


def merge_retriever_edges(merged_edges: dict[EdgeID, Edge], incoming_edges: dict[EdgeID, Edge]) -> None:
    """Merge Retriever KG edges without letting sparse duplicates erase metadata."""
    for edge_id, incoming_edge in incoming_edges.items():
        existing_edge = merged_edges.get(edge_id)
        if existing_edge is None or metadata_weight(incoming_edge) > metadata_weight(existing_edge):
            merged_edges[edge_id] = deepcopy(incoming_edge)


# TODO: This function needs to be reevaluated after the typing refactor
def metadata_weight(entity: Edge | Node | None) -> int:
    """Approximate how much Retriever-provided metadata an entity carries."""
    if entity is None:
        return 0
    weight = 1 # +1 simply for existing
    match entity:
        case Edge() as e:
            weight += len(e.sources)
            weight += len(e.attributes_list)
            weight += len(e.qualifiers_list)
        case Node() as n:
            weight += len(n.attributes_list)
            weight += len(n.categories)
            if n.name:
                weight += 1
    return weight


def query_qualifiers_to_edge_qualifiers(qedge: QEdge) -> list[Qualifier]:
    """Convert the first QEdge qualifier set into KG edge qualifiers."""
    qualifier_constraints = qedge.constraints.qualifiers_list if qedge.constraints else []
    if not qualifier_constraints:
        return []

    return [
        Qualifier(
            qualifier_type_id = qualifier_type_id,
            qualifier_value = qualifier_value
        )
        for qualifier_type_id, qualifier_value in qualifier_constraints[0].items()
    ]


def make_xcrg_inferred_edge(
    ctx: RunContext,
    subject_id: CURIE,
    object_id: CURIE,
    original_qedge: QEdge,
    support_graph_ids: list[str],
) -> tuple[EdgeID, Edge]:
    """Create the final source->target inferred edge supported by TF paths."""
    predicate = (original_qedge.predicates_list or ["biolink:affects"])[0]

    edge_id = make_stable_id(
        "xcrg_inferred_edge",
        {
            "source": subject_id,
            "target": object_id,
            "predicate": predicate,
            "support_graphs": support_graph_ids,
        },
    )

    edge = Edge(
        subject = subject_id,
        predicate = predicate,
        object = object_id,
        qualifiers = query_qualifiers_to_edge_qualifiers(original_qedge) or None,
        knowledge_level = "prediction",
        agent_type = "computational_model",
        attributes = [
            Attribute(
                attribute_type_id = "biolink:support_graphs",
                value = cast(FastJsonValue, support_graph_ids), # TODO
                attribute_source = ctx.config.resource_id
            ),
        ],
        sources = [
            RetrievalSource(
                resource_id = ctx.config.resource_id,
                resource_role = "primary_knowledge_source"
            )
        ]
    )

    return edge_id, edge


def copy_query_bound_node(
    qnode_id: QNodeID,
    node_id: CURIE | None,
    original_qgraph: QueryGraph,
    retriever_nodes: dict[CURIE, Node],
    final_nodes: dict[CURIE, Node],
) -> None:
    """Copy explicit query-node metadata for a pinned answer endpoint."""
    if not node_id or node_id in final_nodes:
        return

    retriever_node = retriever_nodes.get(node_id)
    if retriever_node and retriever_node.categories:
        return

    qnode = original_qgraph.nodes.get(qnode_id)
    if not qnode:
        return

    if node_id not in qnode.ids_list:
        return

    categories = qnode.categories_list
    if not qnode.categories:
        return

    final_nodes[node_id] = Node(
        categories = deepcopy(categories),
        attributes = [],
    )


def copy_retriever_edge_and_nodes(
    edge_id: EdgeID | None,
    retriever_edges: dict[EdgeID, Edge],
    retriever_nodes: dict[CURIE, Node],
    final_edges: dict[EdgeID, Edge],
    final_nodes: dict[CURIE, Node],
    retriever_auxiliary_graphs: AuxiliaryGraphsDict | None = None,
    final_auxiliary_graphs: AuxiliaryGraphsDict | None = None,
    copied_auxiliary_graphs: set[AuxGraphID] | None = None,
) -> bool:
    """Copy a Retriever KG edge and endpoint nodes verbatim when present."""
    if not edge_id or edge_id not in retriever_edges:
        return False

    edge = deepcopy(retriever_edges[edge_id])
    subject_id = edge.subject
    object_id = edge.object

    if not node_is_present_for_evidence(subject_id, retriever_nodes, final_nodes):
        return False
    if not node_is_present_for_evidence(object_id, retriever_nodes, final_nodes):
        return False

    final_edges[edge_id] = edge
    trapi.copy_node(subject_id, retriever_nodes, final_nodes)
    trapi.copy_node(object_id, retriever_nodes, final_nodes)
    copy_retriever_edge_support_graphs(
        edge,
        retriever_edges,
        retriever_nodes,
        cast(dict, retriever_auxiliary_graphs), # TODO
        final_edges,
        final_nodes,
        final_auxiliary_graphs,
        copied_auxiliary_graphs,
    )

    return True


def copy_retriever_edge_support_graphs(
    edge: Edge,
    retriever_edges: dict[EdgeID, Edge],
    retriever_nodes: dict[CURIE, Node],
    retriever_auxiliary_graphs: AuxiliaryGraphsDict,
    final_edges: dict[EdgeID, Edge],
    final_nodes: dict[CURIE, Node],
    final_auxiliary_graphs: AuxiliaryGraphsDict | None,
    copied_auxiliary_graphs: set[AuxGraphID] | None,
) -> None:
    """Copy Retriever auxiliary graphs referenced by edge support_graphs attrs."""
    if final_auxiliary_graphs is None:
        return

    if not retriever_auxiliary_graphs:
        remove_edge_support_graph_attrs(edge)
        return

    if copied_auxiliary_graphs is None:
        copied_auxiliary_graphs = set()

    for support_graph_id in get_edge_support_graph_ids(edge):
        copy_retriever_auxiliary_graph(
            support_graph_id,
            retriever_edges,
            retriever_nodes,
            retriever_auxiliary_graphs,
            final_edges,
            final_nodes,
            final_auxiliary_graphs,
            copied_auxiliary_graphs,
        )

    trim_edge_support_graph_attrs(edge, final_auxiliary_graphs)


def trim_edge_support_graph_attrs(edge: Edge, aux_graph: AuxiliaryGraphsDict) -> None:
    """Drop support graph references that are not present in final aux graphs."""
    trimmed_attributes = []

    for attribute in edge.attributes_list:
        if attribute.attribute_type_id != "biolink:support_graphs":
            trimmed_attributes.append(attribute)
            continue

        value = attribute.value

        if isinstance(value, list):
            copied_values = [
                support_graph_id
                for support_graph_id in value
                if support_graph_id in aux_graph
            ]
            if copied_values:
                copied_attribute = deepcopy(attribute)
                copied_attribute.value = cast(FastJsonValue, copied_values) # TODO
                trimmed_attributes.append(copied_attribute)
        elif isinstance(value, str) and value in aux_graph:
            trimmed_attributes.append(attribute)

    edge.attributes = trimmed_attributes


def remove_edge_support_graph_attrs(edge: Edge) -> None:
    """Remove support_graphs attrs when no Retriever aux graph map is available."""
    edge.attributes = [
        attribute
        for attribute in edge.attributes_list
        if attribute.attribute_type_id != "biolink:support_graphs"
    ]


def get_edge_support_graph_ids(edge: Edge) -> list[AuxGraphID]:
    """Return support graph IDs referenced by a Retriever KG edge."""
    support_graph_ids = list[AuxGraphID]()
    for attribute in edge.attributes_list:
        if attribute.attribute_type_id != "biolink:support_graphs":
            continue
        value = attribute.value
        if isinstance(value, list):
            support_graph_ids.extend(item for item in value if isinstance(item, str))
        elif isinstance(value, str):
            support_graph_ids.append(value)
    return support_graph_ids


def copy_retriever_auxiliary_graph(
    support_graph_id: AuxGraphID,
    retriever_edges: dict[EdgeID, Edge],
    retriever_nodes: dict[CURIE, Node],
    retriever_auxiliary_graphs: AuxiliaryGraphsDict,
    final_edges: dict[EdgeID, Edge],
    final_nodes: dict[CURIE, Node],
    final_auxiliary_graphs: AuxiliaryGraphsDict,
    copied_auxiliary_graphs: set[AuxGraphID],
) -> bool:
    """Copy a Retriever auxiliary graph and its referenced KG edges."""
    if support_graph_id in final_auxiliary_graphs:
        return True
    if support_graph_id in copied_auxiliary_graphs:
        return support_graph_id in final_auxiliary_graphs

    aux_graph = retriever_auxiliary_graphs.get(support_graph_id)
    if not aux_graph:
        return False

    copied_auxiliary_graphs.add(support_graph_id)
    copied_edges = []

    for edge_id in aux_graph.edges:
        # TODO: This if block is pretty gnarly
        if edge_id in final_edges or copy_retriever_edge_and_nodes(
            edge_id,
            retriever_edges,
            retriever_nodes,
            final_edges,
            final_nodes,
            retriever_auxiliary_graphs,
            final_auxiliary_graphs,
            copied_auxiliary_graphs,
        ):
            copied_edges.append(edge_id)

    if not copied_edges:
        return False

    copied_auxiliary_graph = deepcopy(aux_graph)
    copied_auxiliary_graph.edges = copied_edges
    final_auxiliary_graphs[support_graph_id] = copied_auxiliary_graph

    return True


def node_is_present_for_evidence(
    node_id: CURIE | None,
    retriever_nodes: dict[CURIE, Node],
    final_nodes: dict[CURIE, Node],
) -> bool:
    """Return True when an evidence edge has an endpoint node to reference."""
    if not node_id:
        return False
    if node_id in final_nodes:
        return True
    return node_id in retriever_nodes


def add_direct_evidence(final_result: XCRGResult, edge_ids: list[EdgeID]) -> None:
    """Attach direct one-hop KG edge bindings to a final result."""
    for edge_id in edge_ids:
        if edge_id in final_result.xcrg_direct_binding_ids:
            continue
        final_result.xcrg_direct_binding_ids.add(edge_id)
        final_result.xcrg_direct_bindings.append(edge_id)


def add_support_path_edges(final_result: XCRGResult, path_edge_ids: list[EdgeID]) -> None:
    """Collect unique TF-mediated path edges for the final predicted edge."""
    for edge_id in path_edge_ids:
        if edge_id in final_result.xcrg_support_edge_ids:
            continue
        final_result.xcrg_support_edge_ids.add(edge_id)


def finalize_clean_result_analyses(
    ctx: RunContext,
    final_result: XCRGResult,
    original_qgraph: QueryGraph,
    retriever_nodes: dict[CURIE, Node],
    retriever_edges: dict[EdgeID, Edge],
    retriever_auxiliary_graphs: AuxiliaryGraphsDict,
    kg_nodes: dict[CURIE, Node],
    kg_edges: dict[EdgeID, Edge],
    auxiliary_graphs: AuxiliaryGraphsDict,
    original_qedge_id: QEdgeID,
    original_qedge: QEdge,
) -> None:
    """Build final Retriever/xCRG analyses after evidence grouping."""
    source_qnode = original_qedge.subject
    source_id = final_result.node_bindings[source_qnode].ids[0]
    copy_query_bound_node(
        source_qnode,
        source_id,
        original_qgraph,
        retriever_nodes,
        kg_nodes,
    )

    target_qnode = original_qedge.object
    target_id = final_result.node_bindings[target_qnode].ids[0]
    copy_query_bound_node(
        target_qnode,
        target_id,
        original_qgraph,
        retriever_nodes,
        kg_nodes,
    )

    xcrg_bindings = list[EdgeID]()

    direct_bindings = final_result.xcrg_direct_bindings
    for edge_id in direct_bindings:
        if copy_retriever_edge_and_nodes(
            edge_id,
            retriever_edges,
            retriever_nodes,
            kg_edges,
            kg_nodes,
            retriever_auxiliary_graphs,
            auxiliary_graphs,
        ):
            xcrg_bindings.append(edge_id)

    support_edges = final_result.xcrg_support_edge_ids
    copied_support_edges = [
        edge_id
        for edge_id in support_edges
        if copy_retriever_edge_and_nodes(
            edge_id,
            retriever_edges,
            retriever_nodes,
            kg_edges,
            kg_nodes,
            retriever_auxiliary_graphs,
            auxiliary_graphs,
        )
    ]

    if copied_support_edges:
        support_graph_id = make_stable_id(
            "xcrg_support",
            {
                "source": source_id,
                "target": target_id,
                "edges": copied_support_edges,
            },
        )

        auxiliary_graphs[support_graph_id] = AuxiliaryGraph(
            edges = copied_support_edges,
        )

        trapi.copy_node(source_id, retriever_nodes, kg_nodes)
        trapi.copy_node(target_id, retriever_nodes, kg_nodes)

        inferred_edge_id, inferred_edge = make_xcrg_inferred_edge(
            ctx,
            source_id,
            target_id,
            original_qedge,
            [support_graph_id]
        )

        kg_edges[inferred_edge_id] = inferred_edge
        xcrg_bindings.append(inferred_edge_id)

    if xcrg_bindings:
        analysis = Analysis(
            resource_id = ctx.config.resource_id,
            edge_bindings = {
                original_qedge_id: EdgeBinding(ids = xcrg_bindings),
            }
        )
        ngd.add_ngd_analysis_support_graph(
            ctx,
            analysis,
            final_result.ngd_score,
            kg_edges,
            kg_nodes,
            auxiliary_graphs,
            retriever_nodes,
            source_id,
            target_id
        )
        final_result.analyses = [analysis]
    else:
        final_result.analyses = []


def build_trapi_clean_response(ctx: RunContext, old_response: Response) -> Response:
    """Convert debug-shaped direct+2-hop results into one-hop TRAPI results."""
    old_kgraph = old_response.message.knowledge_graph or KnowledgeGraph.new()
    old_kgraph_edges = old_kgraph.edges_dict
    old_aux_graphs = old_response.message.auxiliary_graphs_dict

    new_qgraph = deepcopy(ctx.query_graph)
    new_kgraph = KnowledgeGraph.new()
    new_aux_graphs = AuxiliaryGraphsDict()

    new_results = dict[tuple[CURIE, CURIE], XCRGResult]()

    for old_result in old_response.message.results_list:
        subject_id = trapi.get_bound_node_curie(old_result, ctx.subject_qid)
        object_id = trapi.get_bound_node_curie(old_result, ctx.object_qid)
        if not subject_id or not object_id:
            continue

        key = (subject_id, object_id)

        new_result: XCRGResult
        if key in new_results:
            new_result = new_results[key]
        else:
            new_result = XCRGResult(node_bindings = {
                ctx.subject_qid: NodeBinding(ids = [subject_id]),
                ctx.object_qid:  NodeBinding(ids = [object_id])
            })
            new_results[key] = new_result

        if trapi.is_two_hop_result(old_result):
            path_edge_ids: list[str] = [
                edge_id
                for qedge_id in ("e0", "e1")
                for edge_id in trapi.get_edge_binding_ids(old_result, qedge_id)
            ]
            if path_edge_ids:
                add_support_path_edges(new_result, path_edge_ids)
        else:
            add_direct_evidence(new_result, trapi.get_edge_binding_ids(old_result, DIRECT_QEDGE_ID))


    ranked_results = ranking.rank_results(ctx, old_response, list(new_results.values()))
    total = len(ranked_results)

    final_results = list[Result]()
    for i, result in enumerate(ranked_results):
        finalize_clean_result_analyses(
            ctx,
            result,
            new_qgraph,
            old_kgraph.nodes,
            old_kgraph_edges,
            old_aux_graphs,
            new_kgraph.nodes,
            new_kgraph.edges_dict,
            new_aux_graphs,
            ctx.query_edge_id,
            ctx.query_edge
        )

        # Assign rank-derived TRAPI Analysis.score values
        score = float(total - i) / total
        for analysis in result.analyses:
            if ctx.resource_id != analysis.resource_id:
                continue
            analysis.score = score
            analysis.scoring_method = ctx.scoring_method

        final_results.append(result.to_trapi_result())

    return Response(
        schema_version = old_response.schema_version,
        biolink_version = old_response.biolink_version,
        message = Message(
            query_graph = new_qgraph,
            knowledge_graph = new_kgraph,
            results = final_results,
            auxiliary_graphs = new_aux_graphs or None
        )
    )


async def run_direct_lookup(ctx: RunContext) -> Response:
    """Run a direct (one-hop) xCRG lookup."""
    return await retriever.run_sync_lookup(ctx, ctx.query)


async def run_inferred_lookup(ctx: RunContext) -> Response:
    """Run phase-one TF-mediated inferred xCRG lookup."""
    ctx.debug_dump_json("original_inferred_query", ctx.query, level = DebugLevel.BASIC)

    one_hop_query = build_one_hop_query(ctx)
    ctx.debug_dump_json("direct_lookup_query", one_hop_query)

    direct_response = await retriever.run_sync_lookup(ctx, one_hop_query)
    ctx.debug_dump_json("direct_filtered_response", direct_response)

    final_direction = trapi.get_qualifier_value(ctx.query_edge, "biolink:object_direction_qualifier") or ""
    templates = DIRECTION_TEMPLATES[Direction(final_direction.lower())] # TODO: raise better error message

    tf_batches = chunk_values(ctx.tf_list, ctx.config.tf_batch_size)
    ctx.reporter.info(
        "Running inferred xCRG lookup with %s TFs across %s batches of up to %s IDs.",
        len(ctx.tf_list),
        len(tf_batches),
        ctx.config.tf_batch_size,
    )

    filtered_responses = []
    debug_summary = Debug_Summary(
        query_id = ctx.query_id,
        debug_run_dir = str(ctx.debug_ctx and ctx.debug_ctx.run_dir or ""), # TODO
        final_direction = final_direction,
        tf_count = len(ctx.tf_list),
        batch_size = ctx.config.tf_batch_size,
        batch_count = len(tf_batches),
        direct_response = Message_Statistics.get_from(direct_response)
    )

    async def process_lookup(batch_idx: int, template: Direction_Template, tf_batch: list[CURIE]) -> tuple[Response, Batch_Summary]:
        two_hop_query = build_two_hop_query(ctx, tf_batch, template[0], template[1])
        ctx.debug_dump_json(f"template_{i}_batch_{batch_idx}_query", two_hop_query)

        response = await retriever.run_sync_lookup(ctx, two_hop_query)
        ctx.debug_dump_json(f"template_{i}_batch_{batch_idx}_response", response)

        summary = Batch_Summary(
            batch_index = batch_idx,
            tf_ids = tf_batch,
            tf_count = len(tf_batch),
            response = Message_Statistics.get_from(response)
        )

        return response, summary

    # Batches are processed using asyncio to work around response times from retriever
    for i, template in enumerate(templates, start = 1):
        template_summary = Template_Summary(
            template_index = i,
            first_direction = template[0],
            second_direction = template[1]
        )

        tasks = list[Coroutine[int, list[CURIE], tuple[Response, Batch_Summary]]]()
        for batch_idx, tf_batch in enumerate(tf_batches, start = 1):
            tasks.append(process_lookup(batch_idx, template, tf_batch))

        results: list[tuple[Response, Batch_Summary]] = await asyncio.gather(*tasks)
        for response, summary in results:
            filtered_responses.append(response)
            template_summary.batches.append(summary)

        template_summary.batches.sort(key = lambda x: x.batch_index) # Put back in original order
        debug_summary.templates.append(template_summary)

    merged_query_graph = build_combined_query_graph(ctx)

    qgraph = build_two_hop_query(
        ctx,
        ctx.tf_list,
        templates[0][0],
        templates[0][1],
    ).message.query_graph

    merged_inferred = merge_filtered_responses(
        ctx,
        filtered_responses,
        cast(QueryGraph, qgraph)
    )

    merged = merge_filtered_responses(
        ctx,
        [direct_response, merged_inferred],
        merged_query_graph
    )
    ctx.debug_dump_json("merged_debug_response", merged)

    final_response = build_trapi_clean_response(ctx, merged)
    debug_summary.merged_response = Message_Statistics.get_from(final_response)

    ctx.debug_dump_json("inferred_debug_summary", debug_summary)
    ctx.debug_dump_json("final_response", final_response, level = DebugLevel.BASIC)

    return final_response


def validate_query(query: Query) -> KnowledgeType:
    """Raise an Error if the given query fails to adhere to a valid xCRG query shape."""
    qgraph = query.message.query_graph
    if not isinstance(qgraph, QueryGraph) or qgraph.paths:
        raise ValueError("xCRG query requires a non-pathfinder query graph.")

    _, qedge = trapi.get_single_query_edge(qgraph)

    if "biolink:affects" not in qedge.predicates_list:
        raise ValueError("xCRG query requires a biolink:affects predicate.")

    qnodes = qgraph.nodes

    subject_qnode = qnodes.get(qedge.subject)
    if not subject_qnode:
        raise ValueError("xCRG query edge requires a 'subject' node reference.")
    object_qnode = qnodes.get(qedge.object)
    if not object_qnode:
        raise ValueError("xCRG query edge requires an 'object' node reference.")

    knowledge_type = qedge.knowledge_type or "lookup"

    match knowledge_type: # default per TRAPI spec; subject to change
        # Validate a phase-one inferred xCRG query while preserving user direction.
        case "inferred":
            pinned_count = sum(1 for node in [subject_qnode, object_qnode] if node.ids)
            if pinned_count != 1:
                raise ValueError("xCRG inferred query requires exactly one pinned endpoint node.")

            if "biolink:ChemicalEntity" not in subject_qnode.categories_list:
                raise ValueError("xCRG inferred query requires one 'ChemicalEntity' endpoint.")
            if "biolink:Gene" not in object_qnode.categories_list:
                raise ValueError("xCRG inferred query requires one 'Gene' endpoint.")

            direction = trapi.get_qualifier_value(qedge, "biolink:object_direction_qualifier")
            if direction not in {"increased", "decreased"}:
                raise ValueError("xCRG inferred query direction must be 'increased' or 'decreased'.")

            valid_aspects = biolink.get_valid_aspect_qualifiers()
            aspect = trapi.get_qualifier_value(qedge, "biolink:object_aspect_qualifier")
            if aspect not in valid_aspects:
                raise ValueError(
                    f"Inferred xCRG query requires an 'object_aspect_qualifier' that is a "
                    f"descendant of {biolink.ASPECT_QUALIFIER_ROOT!r} "
                    f"(e.g. {', '.join(sorted(valid_aspects))}); but got {aspect!r}."
                )
        # Validate the direct one-hop xCRG query shape.
        case "lookup":
            pinned_ids = [qid for qid, qnode in qnodes.items() if qnode.ids]
            if len(pinned_ids) != 1:
                raise ValueError("xCRG direct lookup supports exactly one pinned query node.")
            pinned_node = qnodes[pinned_ids[0]]
            if "biolink:Gene" not in pinned_node.categories_list:
                raise ValueError("xCRG direct lookup requires the pinned node to be a 'Gene'.")

            unbound_ids = [qid for qid, qnode in qnodes.items() if not qnode.ids]
            if len(unbound_ids) != 1:
                raise ValueError("xCRG direct lookup supports exactly one unbound query node.")
            unbound_node = qnodes[unbound_ids[0]]
            if "biolink:ChemicalEntity" not in unbound_node.categories_list:
                raise ValueError("xCRG direct lookup requires the unbound node to be a 'ChemicalEntity'.")
        case _:
            raise ValueError("Invalid knowledge type; valid values are 'inferred' or 'lookup'.")

    return knowledge_type


def query_from_dict(query: dict) -> Query:
    """Read a query dict for what xCRG uses: its query graph, parameters and submitter.

    The rest of the message (an ARAX envelope's empty knowledge graph, results and
    auxiliary graphs, which TRAPI 2.0 forbids empty) is not read.
    """
    message = query.get("message") or {}
    return Query.from_dict({
        **{key: query[key] for key in ("parameters", "submitter") if query.get(key) is not None},
        "message": {"query_graph": message.get("query_graph")},
    })


def is_xcrg_mvp2_query(query: dict | Query) -> bool: # TODO: is_valid_query
    try:
        if isinstance(query, dict):
            query = query_from_dict(query)
        validate_query(query)
        return True
    except Exception:
        return False


# TODO: Rename to "async_run_query"
async def async_run_xcrg(
    message: Query | dict | str,
    config: XCRGConfig,
    logger: logging.Logger | Reporter | None = LogReporter(),
    query_id: str | None = None,
) -> dict:
    """Run xCRG and return a complete TRAPI response."""
    with tracer.start_as_current_span("run_query") as span:
        query_id = query_id or uuid.uuid4().hex[:8]
        span.set_attribute("query_id", query_id)

        # TODO: Remove this block when RTX is refactored to use xcrg.Reporter
        #  We cannot use reporter.critical or reporter.fatal while doing this
        if logger and logger.__class__.__name__ == "ARAXXCRGLogger":
            logger = LogReporter(cast(logging.Logger, logger))

        reporter: Reporter
        match logger:
            case Reporter():       reporter = logger
            case logging.Logger(): reporter = LogReporter(logger)
            case _:                reporter = StubReporter()

        query: Query
        match message:
            case Query(): query = message
            case dict():  query = query_from_dict(message)
            case str():   query = query_from_dict(json.loads(message))

        query.submitter = query.submitter or config.resource_id

        ctx = RunContext.new(
            query_id = query_id,
            query = query,
            config = config,
            reporter = reporter
        )

        response: Response
        if validate_query(query) == "inferred":
            response = await run_inferred_lookup(ctx)
        else:
            response = await run_direct_lookup(ctx)

        # to_dict() dumps in pydantic's JSON mode, which turns a NaN attribute value from
        # Retriever into null (which ARAX's models reject); the caller serializes NaN itself
        return response.model_dump(mode = "python", exclude_none = True, exclude_defaults = True)


# TODO: Rename to "run_query"
def run_xcrg(
    message: Query | dict | str,
    config: XCRGConfig,
    logger: logging.Logger | Reporter | None = LogReporter(),
    query_id: str | None = None,
) -> dict:
    """Synchronous wrapper for callers that are not already running an event loop."""
    return asyncio.run(
        async_run_xcrg(
            message=message,
            config=config,
            logger=logger,
            query_id=query_id,
        )
    )
