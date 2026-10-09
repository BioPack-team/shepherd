# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/trapi.py (DEC-21).
# Changes from upstream:
#   - import paths; TRAPI 2.0: qualifiers from constraints.qualifiers, one binding per qnode/qedge (get_edge_bindings is get_edge_binding_ids), pathfinder graphs are QueryGraph.paths
# See README.md.
from copy import deepcopy

from translator_tom import (
    Analysis,
    Biolink,
    CURIE,
    EdgeID,
    Node,
    QEdge,
    QEdgeID,
    QNodeID,
    QueryGraph,
    Result,
)

from .utilities import XCRGResult


def get_single_query_edge(qgraph: QueryGraph | None) -> tuple[QEdgeID, QEdge]:
    """Return the single query edge for xCRG queries."""
    if qgraph is None:
        raise ValueError("Query graph is required.")
    if qgraph.paths:
        raise ValueError("Pathfinder query graphs are not supported.")
    qedges = qgraph.edges_dict
    if len(qedges) != 1:
        raise ValueError("xCRG queries are currently required to have only one query edge.")
    qedge_id = next(iter(qedges))
    return qedge_id, qedges[qedge_id]


def get_qualifier_value(edge: QEdge, qualifier_type_id: Biolink.Qualifier) -> str | None:
    """Return a qualifier value from the first qualifier set, if present."""
    qualifier_constraints = edge.constraints.qualifiers_list if edge.constraints else []
    if not qualifier_constraints:
        return None
    return qualifier_constraints[0].get(qualifier_type_id)


def copy_node(
    node_id: CURIE | None,
    old_nodes: dict[CURIE, Node],
    new_nodes: dict[CURIE, Node],
) -> None:
    """Copy a Retriever-provided KG node verbatim into the final KG."""
    if node_id and node_id in old_nodes and node_id not in new_nodes:
        new_nodes[node_id] = deepcopy(old_nodes[node_id])


def get_edge_binding_ids(result: Result, qedge_id: QEdgeID) -> list[EdgeID]:
    """Return the edge ids bound to a qedge across all analyses."""
    edge_ids = list[EdgeID]()
    seen = set()
    for analysis in result.analyses_list:
        if not isinstance(analysis, Analysis):
            continue
        binding = analysis.edge_bindings_dict.get(qedge_id)
        for edge_id in binding.ids if binding else []:
            if edge_id in seen:
                continue
            seen.add(edge_id)
            edge_ids.append(edge_id)
    return edge_ids


def get_bound_node_curie(result: Result | XCRGResult, qid: QNodeID) -> CURIE | None:
    """Return the first node binding id for the given qnode."""
    binding = result.node_bindings.get(qid)
    if not binding:
        return None
    return binding.ids[0]


def result_edge_binding_keys(result: Result) -> set[str]:
    """Return qedge ids bound by any analysis in the result."""
    keys = set()
    for analysis in result.analyses_list:
        if isinstance(analysis, Analysis):
            keys.update(analysis.edge_bindings_dict.keys())
    return keys


def is_two_hop_result(result: Result) -> bool:
    """Return True for TF-mediated inferred results."""
    keys = result_edge_binding_keys(result)
    return "e0" in keys and "e1" in keys # TODO: hardcoded


def is_two_hop_query(qgraph: QueryGraph) -> bool:
    return "e0" in qgraph.edges_dict and "e1" in qgraph.edges_dict # TODO: hardcoded


def get_answer_qid(
    query_graph: QueryGraph,
    subject_qid: QNodeID,
    object_qid: QNodeID,
) -> QNodeID:
    """Return the unpinned endpoint qnode whose bindings are the answer list."""
    for qid in (subject_qid, object_qid):
        if qnode := query_graph.nodes.get(qid):
            if not qnode.ids:
                return qid
    return object_qid
