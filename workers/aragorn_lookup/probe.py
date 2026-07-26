"""Per-disease neighbourhood probe for template selection.

The census gives mean fan-out per triple, but disease degree varies by orders of
magnitude: a template priced at 238 expected paths against the mean will return
far more for a disease with 200 associated proteins and far fewer for one with
three.  Selecting a portfolio from means alone therefore holds the ~10s budget
on the average disease and blows it in the tail -- which is exactly where it
matters.

This module closes that gap (handoff §6.3).  Before the real expansions are
fired it measures the pinned disease's actual degree on each entry hop the
portfolio uses, with one small synchronous query per distinct hop, run
concurrently.  ``query_templates.estimate`` then substitutes the measured degree
for the census mean on that hop, and budget selection sees per-disease costs
rather than global ones.

Cost control matters here, since this runs on every creative query:

- the probes are ``dehydrated`` (no edge-attribute enrichment) and capped with
  ``max_node_degree``, so a hub disease cannot return a huge payload;
- the whole set is bounded by one wall-clock timeout, not per-request ones;
- any failure returns no measurement for that hop rather than raising, and
  pricing falls back to the census mean.  A probe that does not answer in time
  must never be the reason a query fails.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional, Sequence

import httpx

from shepherd_utils.config import settings

from .query_templates import ProbeSpec

DISEASE_QNODE = "probe_disease"
FAR_QNODE = "probe_far"


def build_probe_query(
    spec: ProbeSpec,
    disease_curie: str,
    max_node_degree: int,
) -> dict:
    """A one-hop TRAPI query measuring one entry hop of the pinned disease."""
    disease_node: dict = {DISEASE_QNODE: {"ids": [disease_curie]}}
    far_node: dict = {FAR_QNODE: {"categories": [spec.category]}}
    if spec.disease_is_subject:
        edge = {"subject": DISEASE_QNODE, "object": FAR_QNODE}
    else:
        edge = {"subject": FAR_QNODE, "object": DISEASE_QNODE}
    edge["predicates"] = list(spec.predicates)
    return {
        "message": {
            "query_graph": {
                "nodes": {**disease_node, **far_node},
                "edges": {"e0": edge},
            }
        },
        "parameters": {
            "dehydrated": True,
            "filter_config": {"max_node_degree": max_node_degree},
        },
    }


def count_neighbours(response: dict) -> int:
    """Distinct nodes bound to the far end of the probe query."""
    results = (response.get("message") or {}).get("results") or []
    seen = set()
    for result in results:
        for binding in (result.get("node_bindings") or {}).get(FAR_QNODE, []):
            node_id = binding.get("id")
            if node_id is not None:
                seen.add(node_id)
    return len(seen)


async def _probe_one(
    client: httpx.AsyncClient,
    spec: ProbeSpec,
    disease_curie: str,
    max_node_degree: int,
    logger: logging.Logger,
) -> tuple[str, Optional[int]]:
    try:
        response = await client.post(
            settings.sync_kg_retrieval_url,
            json=build_probe_query(spec, disease_curie, max_node_degree),
        )
        if response.status_code != 200:
            logger.debug(
                "Probe %s returned %d; falling back to the census mean.",
                spec.key(),
                response.status_code,
            )
            return spec.key(), None
        return spec.key(), count_neighbours(response.json())
    except Exception as error:  # noqa: BLE001 - a probe must never fail a query
        logger.debug("Probe %s failed (%s); using the census mean.", spec.key(), error)
        return spec.key(), None


async def probe_disease(
    disease_curie: str,
    specs: Sequence[ProbeSpec],
    logger: logging.Logger,
    timeout: Optional[float] = None,
    max_node_degree: Optional[int] = None,
) -> dict[str, int]:
    """Measure the pinned disease's degree on each entry hop.

    Returns ``{spec.key(): distinct neighbours}``, omitting any hop that did not
    answer in time.  An empty dict means "no measurements" -- pricing then uses
    census means throughout, which is the pre-probe behaviour.
    """
    if not specs:
        return {}
    timeout = settings.template_probe_timeout if timeout is None else timeout
    max_node_degree = (
        settings.template_probe_max_node_degree
        if max_node_degree is None
        else max_node_degree
    )

    measurements: dict[str, int] = {}
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            done = await asyncio.wait_for(
                asyncio.gather(
                    *(
                        _probe_one(client, spec, disease_curie, max_node_degree, logger)
                        for spec in specs
                    ),
                    return_exceptions=True,
                ),
                timeout=timeout,
            )
    except Exception as error:  # noqa: BLE001 - includes the wait_for timeout
        logger.debug(
            "Disease probe for %s did not finish in %.1fs (%s); "
            "pricing templates from census means.",
            disease_curie,
            timeout,
            type(error).__name__,
        )
        return {}

    for outcome in done:
        if isinstance(outcome, BaseException) or not isinstance(outcome, tuple):
            continue
        key, count = outcome
        if count is not None:
            measurements[key] = count

    if measurements:
        logger.debug(
            "Probed %s: %s",
            disease_curie,
            ", ".join(f"{key}={count}" for key, count in sorted(measurements.items())),
        )
    return measurements
