"""Per-response message hygiene, run before the cross-ARA merge (ARS parity).

Ports three ARS steps that clean and decorate each ARA response prior to merging
(ARS ``pre_merge_process`` + the phantom-support-graph strip that runs just before
validation):

* ``scrub_null_attributes`` -- drop ``None`` attribute entries; ensure edge
  ``sources`` have a non-null ``resource_id`` (drop bad ones) and list-valued
  ``upstream_resource_ids``; default aux graphs to ``attributes: []``.
* ``decorate_edges_with_infores`` -- ensure every KG edge records the responding
  ARA as a knowledge source (primary when the edge has none, else self as an
  aggregator alongside the existing primary).
* ``remove_phantom_support_graphs`` -- strip ``biolink:support_graphs`` references
  that point at auxiliary graphs which don't exist.

All operate in place on a full ``{"message": {...}}`` envelope and are pure/CPU-
bound. Two latent bugs in the reference are fixed here without changing intent: a
fresh source dict is built per edge (the reference shared and mutated one object
across edges), and ``has_primary`` is initialized (the reference could raise
``NameError`` on an edge whose sources lacked a primary).
"""

import logging


def _kg(message: dict) -> dict:
    return (message.get("message") or {}).get("knowledge_graph") or {}


def scrub_null_attributes(message: dict) -> None:
    """Remove null attributes and repair edge sources / aux-graph attributes."""
    kg = _kg(message)
    nodes = kg.get("nodes")
    edges = kg.get("edges")
    aux_graphs = (message.get("message") or {}).get("auxiliary_graphs")

    if nodes:
        for node in nodes.values():
            attributes = node.get("attributes")
            if attributes is not None:
                while None in attributes:
                    attributes.remove(None)

    if edges:
        for edge in edges.values():
            attributes = edge.get("attributes")
            if attributes is not None:
                while None in attributes:
                    attributes.remove(None)
                for attribute in attributes:
                    if "attributes" in attribute and attribute.get("attributes") is None:
                        attribute["attributes"] = []

            sources = edge.get("sources")
            if not sources:
                continue
            sources_to_remove = []
            for source in sources:
                if source.get("resource_id") is None:
                    sources_to_remove.append(source)
                if source.get("upstream_resource_ids") is None:
                    source["upstream_resource_ids"] = []
                elif isinstance(source["upstream_resource_ids"], list):
                    while None in source["upstream_resource_ids"]:
                        source["upstream_resource_ids"].remove(None)
            for source in sources_to_remove:
                sources.remove(source)

    if aux_graphs:
        for aux_graph in aux_graphs.values():
            if aux_graph.get("attributes") is None:
                aux_graph["attributes"] = []


def decorate_edges_with_infores(message: dict, inforesid: str | None) -> None:
    """Ensure every KG edge names the responding ARA as a knowledge source."""
    if inforesid is None:
        inforesid = "infores:unknown"
    edges = _kg(message).get("edges")
    if not edges:
        return
    for edge in edges.values():
        sources = edge.get("sources")
        if not sources:
            edge["sources"] = [
                {
                    "resource_id": inforesid,
                    "resource_role": "primary_knowledge_source",
                    "source_record_urls": None,
                    "upstream_resource_ids": [],
                }
            ]
            continue
        has_self = False
        has_primary = False
        for source in sources:
            if source.get("resource_id") == inforesid:
                has_self = True
            if source.get("resource_role") == "primary_knowledge_source":
                has_primary = True
        if not has_self:
            # Add self as aggregator if a primary already exists, else as primary.
            role = (
                "aggregator_knowledge_source"
                if has_primary
                else "primary_knowledge_source"
            )
            sources.append(
                {
                    "resource_id": inforesid,
                    "resource_role": role,
                    "source_record_urls": None,
                    "upstream_resource_ids": [],
                }
            )


def remove_phantom_support_graphs(message: dict) -> None:
    """Drop biolink:support_graphs references to auxiliary graphs that don't exist."""
    kg = _kg(message)
    edges = kg.get("edges")
    aux_graphs = (message.get("message") or {}).get("auxiliary_graphs")
    if not edges or aux_graphs is None:
        return
    for edge in edges.values():
        attributes = edge.get("attributes")
        if not attributes:
            continue
        removal_list = []
        for attribute in attributes:
            if attribute.get("attribute_type_id") != "biolink:support_graphs":
                continue
            for value in attribute.get("value") or []:
                if value not in aux_graphs and attribute not in removal_list:
                    logging.debug(
                        f"Support graph {value} referenced but not in auxiliary_graphs"
                    )
                    removal_list.append(attribute)
        for bad in removal_list:
            if bad in attributes:
                attributes.remove(bad)
