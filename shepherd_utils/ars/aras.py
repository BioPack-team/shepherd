"""The ARAs the ARS talks to.

The ARS on Shepherd is de-federated: it fans out only to the ARAs this same
Shepherd deployment hosts, and every hop between the two rides the shared
broker (``ars_fanout`` enqueues the ARA's worker task; ``finish_query`` hands
the response back onto ``ars.premerge``). There is no service registry to
consult and nothing to register at runtime, so the roster is this module: one
entry per hosted ARA, keyed by the Shepherd target name that is both the
worker stream its queries enter on and the ``/{name}/...`` prefix the ARA is
served under. ``GET /ars/api/aras`` renders it, with each ARA's live worker
count, so an operator can see what the ARS can currently reach.

The agent names recorded on ARS message rows (``ars_message.agent``) are kept
from the upstream port so the completion arithmetic, merge bookkeeping, and
trace consumers keep seeing the same strings: ``ars-default-agent`` on a
submitted parent, ``ara-shepherd-<name>`` on an ARA's child, ``ars-ars-agent``
on a merged message.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from shepherd_utils.config import settings

logger = logging.getLogger("shepherd.ars.aras")

# The agent recorded on a submitted query (the tree's parent row).
DEFAULT_AGENT = "ars-default-agent"
# The agent recorded on merged messages, and its infores.
MERGE_AGENT = "ars-ars-agent"
MERGE_INFORESID = "infores:ars"


@dataclass(frozen=True)
class ARA:
    """One Shepherd-hosted ARA.

    ``name`` is the Shepherd target: the worker stream a query is enqueued
    on and the URL prefix (``/{name}/asyncquery``) the ARA is served at.
    ``agent`` is the name written to the ARA's child messages; ``inforesid``
    decorates its results and identifies it in notifications and reports.
    """

    name: str
    inforesid: str
    agent: str


ARAS: Tuple[ARA, ...] = (
    ARA("aragorn", "infores:shepherd-aragorn", "ara-shepherd-aragorn"),
    ARA("arax", "infores:shepherd-arax", "ara-shepherd-arax"),
    ARA("bte", "infores:shepherd-bte", "ara-shepherd-bte"),
)

_BY_NAME: Dict[str, ARA] = {a.name: a for a in ARAS}
_BY_AGENT: Dict[str, ARA] = {a.agent: a for a in ARAS}


def by_name(name: Optional[str]) -> Optional[ARA]:
    return _BY_NAME.get(name) if name else None


def by_agent(agent: Optional[str]) -> Optional[ARA]:
    return _BY_AGENT.get(agent) if agent else None


def inforesid_for(agent: Optional[str]) -> str:
    """The infores an agent name stands for ('' when it has none, e.g. the
    parent's ars-default-agent)."""
    ara = by_agent(agent)
    if ara is not None:
        return ara.inforesid
    if agent == MERGE_AGENT:
        return MERGE_INFORESID
    return ""


def agents_for_inforesid_suffix(suffix: str) -> List[str]:
    """Agent names whose infores ends with ``suffix`` (case-insensitive), the
    match ``GET /ars/api/reports/<inforesid>`` makes."""
    needle = (suffix or "").lower()
    out = []
    for ara in ARAS:
        if ara.inforesid.lower().endswith(needle):
            out.append(ara.agent)
    if MERGE_INFORESID.lower().endswith(needle):
        out.append(MERGE_AGENT)
    return out


def _configured_names() -> Optional[List[str]]:
    raw = (settings.ars_enabled_aras or "").strip()
    if not raw:
        return None
    return [part.strip() for part in raw.split(",") if part.strip()]


def enabled_aras() -> List[ARA]:
    """The ARAs a submitted query fans out to.

    Every hosted ARA unless ``settings.ars_enabled_aras`` names a subset;
    a name in that list that is not a hosted ARA is logged and ignored, so
    a typo disables one ARA rather than the whole fan-out.
    """
    names = _configured_names()
    if names is None:
        return list(ARAS)
    out = []
    for name in names:
        ara = by_name(name)
        if ara is None:
            logger.warning(f"ars_enabled_aras names an unknown ARA {name!r}; ignoring")
            continue
        if ara not in out:
            out.append(ara)
    return out


def is_enabled(ara: ARA) -> bool:
    return ara in enabled_aras()


async def live_worker_counts(streams: Iterable[str]) -> Dict[str, int]:
    """How many workers are currently alive on each stream, from the
    heartbeats every ``get_tasks`` worker keeps in the broker.

    An ARA whose entry stream has no live worker will accept a query into
    the broker and never pick it up, so this is what "can currently talk
    to" means for ``GET /ars/api/aras``. A broker error reads as zero for
    every stream rather than failing the listing.
    """
    from shepherd_utils.broker import broker_client
    from shepherd_utils.heartbeat import HEARTBEAT_PREFIX, is_heartbeat_fresh

    counts = {stream: 0 for stream in streams}
    try:
        for stream in counts:
            keys = [
                key
                async for key in broker_client.scan_iter(
                    match=f"{HEARTBEAT_PREFIX}:{stream}:*", count=100
                )
            ]
            if not keys:
                continue
            payloads = await broker_client.mget(keys)
            counts[stream] = sum(1 for raw in payloads if is_heartbeat_fresh(raw))
    except Exception as e:
        logger.warning(f"Could not read worker heartbeats: {e}")
    return counts
