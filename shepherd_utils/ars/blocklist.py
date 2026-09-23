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

TRAPI 2.0 (Shepherd speaks 2.0; upstream is 1.5): bindings are one
``{"ids": [...]}`` object per query node / edge / path, pruned by id; and no
container 2.0 requires to be non-empty is left empty -- a binding that loses
its last id takes its analysis with it, an analysis that loses its last
support graph drops ``support_graphs``, and a message that loses its last
auxiliary graph drops ``auxiliary_graphs``. Log entries carry RFC 3339
timestamps.

The bundled blocklist.json is the upstream config/blocklist.json copied
verbatim from the pinned commit.
"""

import functools
import json
import logging
import pathlib

from shepherd_utils.trapi import binding_ids

from .premerge import add_log_entry, get_safe, log_timestamp

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
                        for nb in node_bindings.values():
                            if (
                                any(i in nodes_to_remove for i in binding_ids(nb))
                                and result not in results_to_remove
                            ):
                                results_to_remove.append(result)

                    analyses = get_safe(result, "analyses")
                    if analyses is not None:
                        analyses_to_remove = []
                        for analysis in analyses:
                            # TRAPI 2.0: one {"ids": [...]} binding per qedge /
                            # qpath. Removed ids are pruned; a binding left
                            # with no ids (minItems 1) takes its analysis with
                            # it -- upstream's rule, which removed the analysis
                            # when its only binding went and pruned otherwise.
                            # (Upstream, pruning a multi-binding list down to
                            # nothing kept the analysis with an empty list.)
                            for bindings_key, removed_ids in (
                                ("edge_bindings", edges_to_remove),
                                # pathfinder path bindings (upstream MDW
                                # 08/17/26). Upstream's removal loop sat OUTSIDE
                                # the per-path loop and reused the loop
                                # variable's name, so it pruned only the last
                                # path id -- and raised an UnboundLocalError
                                # when path_bindings was empty.
                                ("path_bindings", aux_graphs_to_remove),
                            ):
                                bindings = get_safe(analysis, bindings_key)
                                if not bindings:
                                    continue
                                for binding in bindings.values():
                                    ids = binding_ids(binding)
                                    kept = [i for i in ids if i not in removed_ids]
                                    if len(kept) == len(ids):
                                        continue
                                    if kept:
                                        binding["ids"] = kept
                                    elif analysis not in analyses_to_remove:
                                        analyses_to_remove.append(analysis)

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
                                if not support_graphs:
                                    # minItems 1 in TRAPI 2.0
                                    del analysis["support_graphs"]
                        for analysis in analyses_to_remove:
                            analyses_count += 1
                            analyses.remove(analysis)
                        if len(analyses) == 0 and result not in results_to_remove:
                            results_to_remove.append(result)
                for result in results_to_remove:
                    results.remove(result)

            # Message.auxiliary_graphs has minProperties 1 in TRAPI 2.0
            if aux_graphs is not None and not aux_graphs:
                del data["message"]["auxiliary_graphs"]

        list_of_names = []
        for node in removed_nodes:
            if "name" in node.keys():
                list_of_names.append(node["name"])

        add_log_entry(
            data,
            [
                "Removed the following bad nodes: " + str(list_of_names),
                log_timestamp(),
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
                log_timestamp(),
                "DEBUG",
            ],
        )

        return (str(mesg_id), removed_nodes, results_to_remove)
    except Exception as e:
        logger.error(f"Problem with removing results from block list: {e}")
        raise e
