"""Blocklist-based node/edge/result removal.

Ported from NCATSTranslator/Relay @ 3e65975 tr_sys/tr_ars/utils.py
remove_blocked, restructured to operate purely on the data dict (the Django
Message save side effects are hoisted to the ars_merge worker). The
removal cascade -- nodes, their edges, aux graphs whose edges vanished,
support_graph attribute pruning (an edge losing its last support graph is
itself removed), and result/analysis/binding pruning -- follows upstream.

Deliberate divergences from upstream (see docs/ARS_PARITY_REGISTER.md):
  - every accumulator is bound before use. Upstream declared
    ``aux_graphs_to_remove`` / ``nodes_to_remove`` / ``results_to_remove``
    inside the branches that populate them and read them unconditionally at
    the end, so any response without ``auxiliary_graphs`` -- an ordinary
    shape -- died on an UnboundLocalError, and a missing knowledge graph or
    a null ``edges`` map did the same.
  - pathfinder ``path_bindings`` are pruned per path id. Upstream's removal
    loop was dedented out of the per-path loop and shadowed the dict with
    its own values, so it pruned only the last path id and raised on an
    empty ``path_bindings``.
  - analysis-level ``support_graphs`` are matched against removed AUX GRAPH
    ids. Upstream compared them to removed EDGE ids, which never matches, so
    support graphs that really had been removed stayed on the analysis.

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
        # Bound up front: upstream declared these inside the branches that
        # populate them and then read them unconditionally, so a response
        # with no knowledge graph -- or, far more common, one with no
        # auxiliary_graphs -- died on an UnboundLocalError.
        nodes_to_remove = []
        edges_to_remove = []
        aux_graphs_to_remove = []
        results_to_remove = []
        if nodes:
            nodes_to_remove = list(set(blocklist.keys()) & set(nodes.keys()))
            for node in nodes_to_remove:
                removed_nodes.append(nodes[node])
                del nodes[node]

            if edges:
                for edge_id, edge in edges.items():
                    if (
                        edge.get("subject") in nodes_to_remove
                        or edge.get("object") in nodes_to_remove
                    ):
                        edges_to_remove.append(edge_id)

            if aux_graphs:
                for aux_id, aux_graph in aux_graphs.items():
                    aux_edges = get_safe(aux_graph, "edges")
                    if aux_edges is None:
                        continue
                    overlap = list(set(aux_edges) & set(edges_to_remove))
                    if len(overlap) == len(aux_edges):
                        aux_graphs_to_remove.append(aux_id)
                    for edge_id in overlap:
                        aux_edges.remove(edge_id)
                for aux_id in aux_graphs_to_remove:
                    del aux_graphs[aux_id]

                # an edge whose last support graph just vanished goes too
                for edge_id, edge in (edges or {}).items():
                    attributes = get_safe(edge, "attributes")
                    if attributes is None:
                        continue
                    for attribute in attributes:
                        if (
                            attribute.get("attribute_type_id")
                            != "biolink:support_graphs"
                        ):
                            continue
                        value = attribute.get("value")
                        if not isinstance(value, list):
                            continue
                        overlap = list(set(value) & set(aux_graphs_to_remove))
                        if not overlap:
                            continue
                        for graph in overlap:
                            value.remove(graph)
                        if not value and edge_id not in edges_to_remove:
                            edges_to_remove.append(edge_id)
            for edge_id in edges_to_remove:
                del edges[edge_id]

            if results is not None:
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

                            # pathfinder path bindings (upstream MDW 08/17/26).
                            # Upstream's removal loop sat OUTSIDE the per-path
                            # loop and reused the loop variable's name, so it
                            # pruned only the last path id -- and raised an
                            # UnboundLocalError when path_bindings was empty.
                            path_bindings = get_safe(analysis, "path_bindings")
                            if path_bindings is not None:
                                for path_id, bindings in path_bindings.items():
                                    path_bindings_to_remove = []
                                    for path_binding in bindings:
                                        if path_binding["id"] in aux_graphs_to_remove:
                                            if len(bindings) > 1:
                                                path_bindings_to_remove.append(
                                                    path_binding
                                                )
                                            elif analysis not in analyses_to_remove:
                                                analyses_to_remove.append(analysis)
                                    for pr in path_bindings_to_remove:
                                        bindings.remove(pr)

                            # analysis-level support_graphs are AUX GRAPH ids;
                            # upstream checked them against removed EDGE ids,
                            # so a support graph that actually went away was
                            # left dangling on the analysis.
                            support_graphs = get_safe(analysis, "support_graphs")
                            if support_graphs:
                                for sg in [
                                    sg
                                    for sg in support_graphs
                                    if sg in aux_graphs_to_remove
                                ]:
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

        log_json = {
            "nodes": len(nodes_to_remove),
            "edges": len(edges_to_remove),
            "results": len(results_to_remove),
            "auxiliary_graphs": len(aux_graphs_to_remove),
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
