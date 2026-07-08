"""Node canonicalization (normalize-before-merge) shared by the ARS pipeline.

Canonicalizes every knowledge-graph node curie via the node normalizer
(``settings.node_norm``), merging nodes that resolve to the same canonical id and
rewriting edge subject/object and result node bindings to match. Run **per ARA
response before the cross-ARA merge** (ARS ``pre_merge_process``) so the same
entity returned by two ARAs under different curies collapses to one node/answer
during the merge.

On any normalizer failure the message is passed through unchanged (Relay
``canonizeMessage`` parity) so a flaky service never drops results.

Faithful to Relay's ``canonizeMessage``: on re-keying a node to its canonical id
a ``biolink:xref`` attribute (the original id) and, when the normalizer supplies
equivalent identifiers, a ``biolink:same_as`` attribute are appended.
"""

import logging

import httpx

from shepherd_utils.config import settings
from shepherd_utils.shared import combine_unique_dicts

# Node normalizer batch size. The service handles large bodies but chunking keeps
# request size bounded (mirrors how SIPR batches curies).
BATCH_SIZE = 1000


def _normalizer_url() -> str:
    return settings.node_norm.rstrip("/") + "/get_normalized_nodes"


async def get_normalized_nodes(
    curies: list[str], logger: logging.Logger
) -> dict[str, dict]:
    """Return a ``curie -> {id, label, categories, equivalent_identifiers}`` map.

    Returns an empty map on any failure so the caller passes the message through
    unchanged.
    """
    cmap: dict[str, dict] = {}
    url = _normalizer_url()
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            for start in range(0, len(curies), BATCH_SIZE):
                chunk = curies[start : start + BATCH_SIZE]
                response = await client.post(
                    url,
                    json={
                        "curies": chunk,
                        "conflate": True,
                        "drug_chemical_conflate": True,
                    },
                )
                response.raise_for_status()
                for curie, info in (response.json() or {}).items():
                    if not info:
                        continue
                    canonical = (info.get("id") or {}).get("identifier")
                    if not canonical:
                        continue
                    equivalents = [
                        eq.get("identifier")
                        for eq in (info.get("equivalent_identifiers") or [])
                        if eq.get("identifier")
                    ]
                    cmap[curie] = {
                        "id": canonical,
                        "label": (info.get("id") or {}).get("label"),
                        "categories": info.get("type") or [],
                        "equivalent_identifiers": equivalents,
                    }
    except Exception as e:
        logger.error(f"Node normalization request failed; passing through: {e}")
        return {}
    return cmap


def _canonization_attributes(old_id: str, canon: dict) -> list[dict]:
    """xref (original id) + same_as (equivalent ids) attributes for a re-keyed node."""
    attributes = [
        {
            "attribute_type_id": "biolink:xref",
            "original_attribute_name": "original_id",
            "value": [old_id],
            "value_type_id": "metatype:NodeIdentifier",
            "attribute_source": None,
            "value_url": None,
            "description": None,
        }
    ]
    equivalents = canon.get("equivalent_identifiers")
    if equivalents:
        attributes.append(
            {
                "attribute_type_id": "biolink:same_as",
                "original_attribute_name": "equivalent_identifiers",
                "value": list(equivalents),
                "value_type_id": "metatype:NodeIdentifier",
                "attribute_source": None,
                "value_url": None,
                "description": None,
            }
        )
    return attributes


def canonize_message(message: dict, cmap: dict[str, dict], logger: logging.Logger):
    """Rewrite node ids to their canonical forms, merging duplicates in place."""
    msg = message.get("message", {}) or {}
    kg = msg.get("knowledge_graph") or {"nodes": {}, "edges": {}}
    old_nodes = kg.get("nodes") or {}

    new_nodes: dict[str, dict] = {}
    for old_id, node in old_nodes.items():
        canon = cmap.get(old_id)
        target_id = canon["id"] if canon else old_id
        if canon:
            if canon.get("label"):
                node["name"] = canon["label"]
            if canon.get("categories"):
                existing = node.get("categories") or []
                node["categories"] = list(set(existing) | set(canon["categories"]))
            if old_id != target_id:
                # Record the original id + synonyms before the id is dropped.
                node["attributes"] = (node.get("attributes") or []) + (
                    _canonization_attributes(old_id, canon)
                )
        existing_node = new_nodes.get(target_id)
        if existing_node is None:
            new_nodes[target_id] = node
        else:
            # Two source ids collapsed onto one canonical node: merge fields.
            ec = existing_node.get("categories") or []
            nc = node.get("categories") or []
            if ec or nc:
                existing_node["categories"] = list(set(ec) | set(nc))
            existing_node["attributes"] = combine_unique_dicts(
                existing_node.get("attributes") or [],
                node.get("attributes") or [],
                logger,
            )
            if not existing_node.get("name") and node.get("name"):
                existing_node["name"] = node["name"]
    kg["nodes"] = new_nodes

    for edge in (kg.get("edges") or {}).values():
        if edge.get("subject") in cmap:
            edge["subject"] = cmap[edge["subject"]]["id"]
        if edge.get("object") in cmap:
            edge["object"] = cmap[edge["object"]]["id"]

    for result in msg.get("results") or []:
        for _, bindings in (result.get("node_bindings") or {}).items():
            for binding in bindings:
                if binding.get("id") in cmap:
                    binding["id"] = cmap[binding["id"]]["id"]

    return message


async def normalize_message(message: dict, logger: logging.Logger) -> bool:
    """Canonicalize a message's node curies in place. Returns True if it changed.

    False means "nothing normalized" (no nodes, or the normalizer returned
    nothing / failed) -- the caller should leave the message untouched.
    """
    nodes = (message.get("message", {}).get("knowledge_graph", {}) or {}).get(
        "nodes", {}
    ) or {}
    curies = list(nodes.keys())
    if not curies:
        return False
    cmap = await get_normalized_nodes(curies, logger)
    if not cmap:
        logger.info("Normalizer returned nothing; leaving message unchanged.")
        return False
    canonize_message(message, cmap, logger)
    logger.info(f"Normalized {len(cmap)}/{len(curies)} nodes.")
    return True
