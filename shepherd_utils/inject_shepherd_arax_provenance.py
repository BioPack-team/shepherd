from __future__ import annotations

from typing import Any, Dict


SHEPHERD_ARAX_SOURCE = {
    "resource_id": "infores:shepherd-arax",
    "resource_role": "aggregator_knowledge_source",
    "source_record_urls": None,
    "upstream_resource_ids": ["infores:arax"],
}


def add_shepherd_arax_to_edge_sources(trapi_response: Dict[str, Any]) -> Dict[str, Any]:
    message = trapi_response.get("message")
    if not isinstance(message, dict):
        return trapi_response

    kg = message.get("knowledge_graph")
    if not isinstance(kg, dict):
        return trapi_response

    edges = kg.get("edges")
    if not isinstance(edges, dict):
        return trapi_response

    for edge_id, edge_obj in edges.items():
        if not isinstance(edge_obj, dict):
            continue

        sources = edge_obj.get("sources")
        if sources is None:
            edge_obj["sources"] = [dict(SHEPHERD_ARAX_SOURCE)]
            continue

        if not isinstance(sources, list):
            continue

        already_present = any(
            isinstance(s, dict) and s.get("resource_id") == "infores:shepherd-arax"
            for s in sources
        )
        if not already_present:
            sources.append(dict(SHEPHERD_ARAX_SOURCE))

    return trapi_response
