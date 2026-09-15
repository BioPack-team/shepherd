"""The broker handoff between the ARS and Shepherd's ARAs.

The ARS and the ARA pipelines share one Shepherd deployment (one broker, one
blob store), so a query never leaves the process boundary over HTTP: the
fanout enqueues the ARA's worker task directly, and when the ARA pipeline
finishes, ``finish_query`` hands the response back by enqueueing an intake
task on ``ars.premerge`` instead of POSTing it anywhere.

What ties the two ends together is the callback "URL" stored on the ARA-side
query row: for an ARS-originated query it is the sentinel built here, which
carries the ARS child pk the response belongs to. ``finish_query`` recognizes
the sentinel and routes the response onto the queue; every other callback
value is a real URL and is POSTed as usual.
"""

from typing import Optional
from uuid import UUID

# Scheme deliberately not http(s): if anything ever tries to POST it, httpx
# fails loudly instead of a request going somewhere unintended.
HANDOFF_PREFIX = "shepherd-ars://callback/"


def handoff_callback_url(child_pk) -> str:
    """The sentinel callback stored on an ARS-dispatched query."""
    return f"{HANDOFF_PREFIX}{child_pk}"


def parse_handoff_callback(callback_url: Optional[str]) -> Optional[str]:
    """The ARS child pk carried by a sentinel callback URL, or None."""
    if not callback_url or not callback_url.startswith(HANDOFF_PREFIX):
        return None
    pk = callback_url[len(HANDOFF_PREFIX) :]
    try:
        UUID(pk)
    except ValueError:
        return None
    return pk
