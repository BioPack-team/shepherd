"""Submitting lookups to the retrieval service and waiting for their callbacks.

``aragorn_lookup``, ``bte_lookup`` and ``aragorn_pathfinder`` each carried their
own copy of this (#223 C9-5), and the copies had drifted: only some bounded
their in-flight submissions, and the non-inferred path in the two lookup
workers registered its callback and POSTed with no error handling at all
(C6-3). A rejected or failed submission left its callback registered, so the
query waited out its whole lookup timeout for a callback that was never coming,
and then finished as though the lookup had simply found nothing.

Every lookup now goes through :func:`submit_lookups`:

- each submission registers its callback id (carrying the submission's trace
  context) before it is sent;
- a submission that failed outright has its callback id dropped, so nobody
  waits on it;
- one the service received but never acknowledged keeps its callback id, since
  the service may still deliver it.

If every submission failed outright the query fails loudly (502), instead of
waiting on nothing and reporting an empty answer as a successful one.
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional

import httpx
from opentelemetry import trace
from opentelemetry.propagate import inject

from .config import settings
from .db import (
    abandon_lookup_if_data_lost,
    add_callback_id,
    cleanup_callbacks,
    get_running_callbacks,
    new_id,
    remove_callback_id,
    wait_for_lookups,
)
from .query_errors import QueryError

tracer = trace.get_tracer(__name__)

# How many lookup submissions one process has in flight at once, across every
# task it runs (#221, C6-8). Each holds a Postgres connection while it
# registers its callback; half the pool leaves the rest for the callback wait
# and the other queries. An inferred query fans out to dozens of lookups and a
# pod runs up to 100 tasks, so unbounded that was thousands of concurrent
# inserts against a pool of a few.
MAX_LOOKUPS_IN_FLIGHT = max(1, settings.postgres_pool_max_size // 2)
_lookup_slots: Optional[asyncio.Semaphore] = None
_slots_loop: Optional[asyncio.AbstractEventLoop] = None


def _slots() -> asyncio.Semaphore:
    """The process-wide submission budget, on the running loop."""
    global _lookup_slots, _slots_loop
    loop = asyncio.get_running_loop()
    if _lookup_slots is None or _slots_loop is not loop:
        _lookup_slots = asyncio.Semaphore(MAX_LOOKUPS_IN_FLIGHT)
        _slots_loop = loop
    return _lookup_slots


@dataclass
class AsyncResponse:
    status_code: int
    success: bool
    callback_id: str
    error: Optional[str] = None
    # True when the submission was sent but we never heard the ACK. The
    # retrieval service may well be running the lookup and will POST the
    # callback later, so the callback id must be kept or the results will be
    # rejected on arrival. False means the request never reached the service.
    in_flight: bool = False


async def run_async_lookup(
    client: httpx.AsyncClient,
    message: dict,
    query_id: str,
    ara: str,
    logger: logging.Logger,
    span_name: str = "lookup",
    log_level: int = logging.INFO,
) -> AsyncResponse:
    """Register a callback for ``message`` and submit it.

    ``ara`` picks the callback route (``/<ara>/callback/<id>``).
    """
    callback_id = new_id()
    with tracer.start_as_current_span(span_name) as span:
        span.set_attribute("callback.id", callback_id)
        lookup_carrier = {}
        inject(lookup_carrier)
        # Put callback UID and query ID in postgres
        await add_callback_id(query_id, callback_id, json.dumps(lookup_carrier), logger)

        message["callback"] = f"{settings.callback_host}/{ara}/callback/{callback_id}"

        # Tagged with the callback id: this is the head of the trail for one
        # retrieval, and the callback handler, the retrieval's own logs and the
        # merge all carry the same tag on the way back.
        logger.log(
            log_level,
            f"[{callback_id}] Sending lookup query to {settings.kg_retrieval_url}",
        )
        try:
            response = await client.post(settings.kg_retrieval_url, json=message)
            return AsyncResponse(
                status_code=response.status_code,
                success=response.status_code == 200,
                callback_id=callback_id,
                error=(
                    None
                    if response.status_code == 200
                    else f"HTTP {response.status_code}"
                ),
            )
        except httpx.ReadTimeout as e:
            # The request went out and the service just didn't ACK in time.
            # It has the query and the callback URL, so treat it as still
            # running rather than failed.
            span.record_exception(e)
            return AsyncResponse(
                status_code=500,
                success=False,
                callback_id=callback_id,
                error=f"{type(e).__name__}: no ACK within {client.timeout.read}s",
                in_flight=True,
            )
        except Exception as e:
            span.record_exception(e)
            return AsyncResponse(
                status_code=500,
                success=False,
                callback_id=callback_id,
                error=f"{type(e).__name__}: {e}",
            )


async def _bounded_lookup(client, message, query_id, ara, logger, span_name, level):
    async with _slots():
        return await run_async_lookup(
            client, message, query_id, ara, logger, span_name, level
        )


async def submit_lookups(
    messages: Iterable[dict],
    query_id: str,
    ara: str,
    logger: logging.Logger,
    span_name: str = "lookup",
    log_level: int = logging.INFO,
) -> List[AsyncResponse]:
    """Submit every message, ``MAX_LOOKUPS_IN_FLIGHT`` per process at a time.

    Drops the callback ids of submissions that failed outright. Raises a 502
    ``QueryError`` when there were lookups to send and none of them reached
    the retrieval service.
    """
    messages = list(messages)
    if not messages:
        return []
    async with httpx.AsyncClient(
        timeout=settings.kg_retrieval_submit_timeout
    ) as client:
        responses = await asyncio.gather(
            *(
                _bounded_lookup(
                    client, message, query_id, ara, logger, span_name, log_level
                )
                for message in messages
            ),
            return_exceptions=True,
        )
    accepted = 0
    errors = []
    for response in responses:
        if not isinstance(response, AsyncResponse):
            # An exception before the submission was built (e.g. registering
            # the callback): there is no callback id to drop.
            errors.append(f"{type(response).__name__}: {response}")
            logger.error(
                f"Failed to do lookup and unable to remove callback id: {response}"
            )
            continue
        if response.success:
            accepted += 1
            continue
        if response.in_flight:
            accepted += 1
            logger.warning(
                f"[{response.callback_id}] Lookup submitted but not "
                f"acknowledged, waiting for its callback anyway: {response.error}"
            )
            continue
        errors.append(response.error or f"HTTP {response.status_code}")
        logger.error(
            f"[{response.callback_id}] Failed to do lookup, "
            f"removing callback id: {response.error}"
        )
        await remove_callback_id(response.callback_id, logger)
    if not accepted:
        raise QueryError(
            f"All {len(messages)} lookup(s) to the retrieval service failed: "
            f"{errors[0]}",
            status_code=502,
        )
    return responses


async def wait_for_callbacks(
    query_id: str, response_id: str, max_query_time: float, logger: logging.Logger
) -> None:
    """Wait until every lookup's callback is merged, or ``max_query_time``
    runs out; on timeout, drop the ones still outstanding."""
    running_callback_ids = await wait_for_lookups(
        query_id,
        response_id,
        max_query_time,
        logger,
        get_running_callbacks,
        abandon_lookup_if_data_lost,
    )
    if running_callback_ids:
        logger.warning(
            f"Timed out getting lookup callbacks. {len(running_callback_ids)} "
            f"queries were still running...{running_callback_ids}"
        )
        await cleanup_callbacks(query_id, logger)
