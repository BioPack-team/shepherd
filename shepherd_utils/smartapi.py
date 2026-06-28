"""SmartAPI-backed ARS actor auto-discovery.

Fetches the SmartAPI registry and upserts an ``ars_actors`` row per
(ARA infores x maturity), the Shepherd analog of Relay's ``SmartApiDiscover``.
``parse_smartapi_hits`` is pure so it can be unit-tested without the network.
"""

import logging
from typing import Any, Dict, List

import httpx

from shepherd_utils.config import settings
from shepherd_utils.db import upsert_actor, upsert_agent, upsert_channel


def parse_smartapi_hits(hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Turn SmartAPI registry hits into a flat list of actor dicts.

    One actor per (infores, server maturity). Hits without a translator infores
    or any server are skipped.
    """
    actors: List[Dict[str, Any]] = []
    for hit in hits or []:
        info = hit.get("info") or {}
        translator = info.get("x-translator") or {}
        infores = translator.get("infores")
        if not infores:
            continue
        component = translator.get("component") or "ARA"
        agent_name = info.get("title") or infores
        servers = hit.get("servers") or []
        for server in servers:
            url = server.get("url")
            if not url:
                continue
            maturity = (
                server.get("x-maturity")
                or translator.get("x-maturity")
                or "production"
            )
            actors.append(
                {
                    "infores": infores,
                    "url": url,
                    "channel": component,
                    "agent_name": agent_name,
                    "maturity": maturity,
                }
            )
    return actors


async def fetch_smartapi_hits(logger: logging.Logger) -> List[Dict[str, Any]]:
    """Fetch raw hits from the SmartAPI registry. Returns [] on failure."""
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(settings.smartapi_registry_url)
            response.raise_for_status()
            data = response.json() or {}
    except Exception as e:
        logger.error(f"Failed to fetch SmartAPI registry: {e}")
        return []
    # The registry returns either {"hits": [...]} or a bare list depending on
    # the ``raw`` flag; handle both.
    if isinstance(data, dict):
        return data.get("hits") or []
    if isinstance(data, list):
        return data
    return []


async def refresh_actors(logger: logging.Logger) -> int:
    """Refresh the actor registry from SmartAPI. Returns the number upserted."""
    hits = await fetch_smartapi_hits(logger)
    actors = parse_smartapi_hits(hits)
    for actor in actors:
        await upsert_actor(
            actor["infores"],
            actor["url"],
            actor["channel"],
            actor["agent_name"],
            actor["maturity"],
            logger,
        )
        # Keep the agent/channel registries in sync with discovered actors.
        await upsert_agent(actor["agent_name"], logger, uri=actor["url"])
        await upsert_channel(actor["channel"], logger)
    logger.info(f"Refreshed {len(actors)} ARS actors from SmartAPI.")
    return len(actors)
