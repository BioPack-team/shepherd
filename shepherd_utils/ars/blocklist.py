"""Blocklist-based node/edge/result removal.

Ported from NCATSTranslator/Relay @ dd1e71b tr_sys/tr_ars/utils.py
remove_blocked, restructured to operate purely on the data dict (the Django
Message save side effects are hoisted to the ars_postprocess worker). The
removal cascade -- nodes, their edges, aux graphs whose edges vanished,
support_graph attribute pruning (an edge losing its last support graph is
itself removed), result/analysis/binding pruning including the pathfinder
path_bindings handling and the quirk that analysis-level support_graphs are
checked against removed EDGE ids -- is byte-faithful and golden-tested.

The bundled blocklist.json is the upstream config/blocklist.json copied
verbatim from the pinned commit.
"""

import functools
import json
import logging
import pathlib

from .premerge import add_log_entry, get_safe, timestamp_hms

logger = logging.getLogger(__name__)

BLOCKLIST_PATH = pathlib.Path(__file__).resolve().parent / "blocklist.json"


@functools.lru_cache(maxsize=1)
def load_blocklist() -> dict:
    with open(BLOCKLIST_PATH) as f:
        return json.load(f)


def remove_blocked(data, blocklist=None, mesg_id=""):
    """Mutates ``data`` in place; returns (mesg_id, removed_nodes,
    results_to_remove) exactly like the upstream report tuple."""
    try:
        if blocklist is None:
            blocklist = load_blocklist()
        results = get_safe(data, "message", "results")
        nodes = get_safe(data, "message", "knowledge_graph", "nodes")
        edges = get_safe(data, "message", "knowledge_graph", "edges")
        aux_graphs = get_safe(data, "message", "auxiliary_graphs")
        analyses_count = 0
        removed_nodes = []
        if nodes is not None:
            nodes_to_remove = list(set(blocklist.keys()) & set(nodes.keys()))
            for node in nodes_to_remove:
                removed_nodes.append(nodes[node])
                del nodes[node]

            edges_to_remove = []
            for edge_id, edge in edges.items():
                if (
                    edge["subject"] in nodes_to_remove
                    or edge["object"] in nodes_to_remove
                ):
                    edges_to_remove.append(edge_id)

            if aux_graphs is not None:
                aux_graphs_to_remove = []
                for aux_id, aux_graph in aux_graphs.items():
                    aux_edges = get_safe(aux_graph, "edges")
                    overlap = list(set(aux_edges) & set(edges_to_remove))
                    if len(overlap) == len(aux_edges):
                        aux_graphs_to_remove.append(aux_id)
                    if len(overlap) > 0:
                        for edge_id in overlap:
                            aux_edges.remove(edge_id)
                for aux_id in aux_graphs_to_remove:
                    del aux_graphs[aux_id]

                for edge_id, edge in edges.items():
                    if "attributes" in edge.keys() and edge["attributes"] is not None:
                        attributes = get_safe(edge, "attributes")
                        for attribute in attributes:
                            if "attribute_type_id" in attribute.keys():
                                type_id = attribute["attribute_type_id"]
                                if (
                                    type_id is not None
                                    and type_id == "biolink:support_graphs"
                                ):
                                    overlap = list(
                                        set(attribute["value"])
                                        & set(aux_graphs_to_remove)
                                    )
                                    if len(overlap) > 0:
                                        for graph in overlap:
                                            attribute["value"].remove(graph)
                                        if (
                                            len(attribute["value"]) == 0
                                            and edge_id not in edges_to_remove
                                        ):
                                            edges_to_remove.append(edge_id)
            for edge_id in edges_to_remove:
                del edges[edge_id]

            if results is not None:
                results_to_remove = []
                for result in results:
                    node_bindings = get_safe(result, "node_bindings")
                    if node_bindings is not None:
                        for k in node_bindings.keys():
                            nb = node_bindings[k]
                            for c in nb:
                                the_id = get_safe(c, "id")
                                if (
                                    the_id in nodes_to_remove
                                    and result not in results_to_remove
                                ):
                                    results_to_remove.append(result)

                    analyses = get_safe(result, "analyses")
                    if analyses is not None:
                        analyses_to_remove = []
                        for analysis in analyses:
                            edge_bindings = get_safe(analysis, "edge_bindings")
                            if edge_bindings is not None:
                                for edge_id, bindings in edge_bindings.items():
                                    bindings_to_remove = []
                                    for binding in bindings:
                                        if binding["id"] in edges_to_remove:
                                            if len(bindings) > 1:
                                                bindings_to_remove.append(binding)
                                            elif analysis not in analyses_to_remove:
                                                analyses_to_remove.append(analysis)
                                    for br in bindings_to_remove:
                                        bindings.remove(br)

                            # pathfinder path bindings (upstream MDW 08/17/26)
                            path_bindings = get_safe(analysis, "path_bindings")
                            if path_bindings is not None:
                                for path_id, path_bindings in path_bindings.items():
                                    path_bindings_to_remove = []
                                    for path_binding in path_bindings:
                                        if path_binding["id"] in aux_graphs_to_remove:
                                            if len(path_bindings) > 1:
                                                path_bindings_to_remove.append(
                                                    path_binding
                                                )
                                            elif analysis not in analyses_to_remove:
                                                analyses_to_remove.append(analysis)
                                for pr in path_bindings_to_remove:
                                    path_bindings.remove(pr)

                            support_graphs = get_safe(analysis, "support_graphs")
                            support_graphs_to_remove = []
                            if support_graphs is not None and len(support_graphs) > 0:
                                # upstream checks against removed EDGE ids
                                for sg in support_graphs:
                                    if sg in edges_to_remove:
                                        support_graphs_to_remove.append(sg)
                                for sg in support_graphs_to_remove:
                                    support_graphs.remove(sg)
                        for analysis in analyses_to_remove:
                            analyses_count += 1
                            analyses.remove(analysis)
                        if len(analyses) == 0 and result not in results_to_remove:
                            results_to_remove.append(result)
                for result in results_to_remove:
                    results.remove(result)

        list_of_names = []
        for node in removed_nodes:
            if "name" in node.keys():
                list_of_names.append(node["name"])

        add_log_entry(
            data,
            [
                "Removed the following bad nodes: " + str(list_of_names),
                timestamp_hms(),
                "DEBUG",
            ],
        )

        aux_count = len(aux_graphs_to_remove)
        nodes_count = len(nodes_to_remove)
        edges_count = len(edges_to_remove)
        results_count = len(results_to_remove)

        log_json = {
            "nodes": nodes_count,
            "edges": edges_count,
            "results": results_count,
            "auxiliary_graphs": aux_count,
            "analyses": analyses_count,
        }
        add_log_entry(
            data,
            [
                "Removed the following counts: " + str(log_json),
                timestamp_hms(),
                "DEBUG",
            ],
        )

        return (str(mesg_id), removed_nodes, results_to_remove)
    except Exception as e:
        logger.error(f"Problem with removing results from block list: {e}")
        raise e
