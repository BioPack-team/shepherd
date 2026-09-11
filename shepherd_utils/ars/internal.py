"""Internal dispatch between the ARS and Shepherd's own ARAs.

When the ARS and the ARA pipelines share one Shepherd deployment (one broker,
one blob store), routing a query through the public HTTP endpoints means two
multi-MB bodies cross the network for nothing: the fanout POSTs the query to
/{ara}/asyncquery on this same cluster, and finish_query POSTs the finished
response back to /ars/api/messages/<child_pk>. This module carries the two
pieces that let both hops stay in-process (documented deviation in the parity
register):

  - the inforesid -> worker-stream map for the ARAs this deployment hosts,
    so ars_fanout can enqueue the ARA's task directly, and
  - the sentinel callback "URL" stored on the ARA-side query row, so
    finish_query recognizes an ARS-bound response and enqueues it onto
    ars.premerge (which runs the callback intake) instead of POSTing it.

The public endpoints stay served either way -- external callers and external
ARAs are unaffected, and ``settings.ars_internal_dispatch`` turns the
short-circuit off entirely. Sentinel URLs already stored keep working while
the flag is off: honoring them is the only way those responses can land.
"""

from typing import Optional
from uuid import UUID

from shepherd_utils.config import settings

# The ARAs hosted by this Shepherd deployment: actor inforesid -> the worker
# stream its queries enter on (the same stream the /{target}/asyncquery
# endpoint feeds). Every other actor is dispatched over HTTP as before.
INTERNAL_ARA_TARGETS = {
    "infores:shepherd-aragorn": "aragorn",
    "infores:shepherd-arax": "arax",
    "infores:shepherd-bte": "bte",
}

# Scheme deliberately not http(s): if a stale finish_query ever tries to POST
# it, httpx fails loudly instead of a request going somewhere unintended.
CALLBACK_PREFIX = "shepherd-ars://callback/"


def internal_target(inforesid: Optional[str]) -> Optional[str]:
    """Worker stream for an internally-hosted ARA, or None to use HTTP."""
    if not settings.ars_internal_dispatch or not inforesid:
        return None
    return INTERNAL_ARA_TARGETS.get(inforesid)


def internal_callback_url(child_pk) -> str:
    """The sentinel callback stored on an internally-dispatched query."""
    return f"{CALLBACK_PREFIX}{child_pk}"


def parse_internal_callback(callback_url: Optional[str]) -> Optional[str]:
    """The ARS child pk carried by a sentinel callback URL, or None.

    Not gated on ``ars_internal_dispatch``: a query dispatched while the flag
    was on must still deliver internally after the flag flips, because its
    sentinel is not POSTable.
    """
    if not callback_url or not callback_url.startswith(CALLBACK_PREFIX):
        return None
    pk = callback_url[len(CALLBACK_PREFIX) :]
    try:
        UUID(pk)
    except ValueError:
        return None
    return pk
