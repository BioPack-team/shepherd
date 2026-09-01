"""ARAX entry module."""

import asyncio
import json
import logging
import uuid
import httpx
from opentelemetry.trace import get_current_span
from shepherd_utils.inject_shepherd_arax_provenance import (
    add_shepherd_arax_to_edge_sources,
)

from shepherd_utils.config import settings
from shepherd_utils.db import get_message, save_message
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, run_task_lifecycle

# Queue name
STREAM = "arax"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
ARAX_TIMEOUT = 270
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)
# How much of a failing ARAX response body goes into the error. The body of a
# non-2xx can be an arbitrarily large HTML error page, and this string ends up
# in the query's logs, so keep only the head of it.
ERROR_BODY_BYTES = 500
# Used when ARAX never answered at all, so there is no status code of its own to
# pass on: we are a gateway in front of it, and these are the codes that say so.
BAD_GATEWAY = 502
GATEWAY_TIMEOUT = 504


class ARAXServiceError(Exception):
    """The ARAX service did not return a usable TRAPI response.

    Carries the upstream HTTP status code so it survives the whole way out:
    onto the task span as ``arax.status_code``, into the query's logs via
    ``run_task_lifecycle``, and so into what the caller gets back. Previously
    the status code was only logged and the failure was swallowed into a
    non-TRAPI ``{"status": "error"}`` blob that the workflow then reported as a
    successful response, which left nothing downstream with a status code to
    report (see the same fix in ``arax_pathfinder``).
    """

    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.status_code = status_code


def body_head(response: httpx.Response) -> str:
    """The first ``ERROR_BODY_BYTES`` of a response body, for an error message."""
    try:
        body = response.content[:ERROR_BODY_BYTES]
    except Exception:
        # Body not readable (streamed/closed response) -- the status code is
        # still worth reporting on its own.
        return ""
    if not body:
        return ""
    return f": {body.decode('utf-8', 'replace')}"


def is_pathfinder_query(message):
    try:
        # this can still fail if the input looks like e.g.:
        #  "query_graph": None
        qedges = message.get("message", {}).get("query_graph", {}).get("edges", {})
    except:
        qedges = {}
    try:
        # this can still fail if the input looks like e.g.:
        #  "query_graph": None
        qpaths = message.get("message", {}).get("query_graph", {}).get("paths", {})
    except:
        qpaths = {}
    if len(qpaths) > 1:
        raise Exception("Only a single path is supported", 400)
    if (len(qpaths) > 0) and (len(qedges) > 0):
        raise Exception("Mixed mode pathfinder queries are not supported", 400)
    return len(qpaths) == 1


async def call_arax(message: dict, logger: logging.Logger) -> dict:
    """POST the message to the ARAX service and return its TRAPI response.

    Raises ``ARAXServiceError`` -- carrying ARAX's own status code -- for
    anything that isn't a parseable 2xx, so the code reaches the span, the
    query's logs and the response instead of being logged and dropped.
    """
    if "submitter" not in message:
        message["submitter"] = (
            "infores:shepherd-arax:{maturity}@{location}@{url}".format(
                maturity=settings.server_maturity,
                location=settings.server_location,
                url=settings.server_url,
            )
        )
    logger.info(f"Get the message from db {message}")
    headers = {"Content-Type": "application/json"}
    span = get_current_span()
    try:
        async with httpx.AsyncClient(timeout=ARAX_TIMEOUT) as client:
            response = await client.post(
                settings.arax_url, json=message, headers=headers
            )
    except httpx.TimeoutException as e:
        span.set_attribute("arax.status_code", GATEWAY_TIMEOUT)
        raise ARAXServiceError(
            f"ARAX service at {settings.arax_url} did not respond within "
            f"{ARAX_TIMEOUT}s: {type(e).__name__}",
            GATEWAY_TIMEOUT,
        ) from e
    except Exception as e:
        # httpx reports connect failures, TLS errors and protocol errors as
        # distinct classes, several of which stringify to an empty message --
        # hence the type name alongside the message.
        span.set_attribute("arax.status_code", BAD_GATEWAY)
        raise ARAXServiceError(
            f"Error occurred calling ARAX service at {settings.arax_url}: "
            f"{type(e).__name__}: {e}",
            BAD_GATEWAY,
        ) from e

    status_code = response.status_code
    span.set_attribute("arax.status_code", status_code)
    logger.info(f"Status Code from ARAX response: {status_code}")
    if not response.is_success:
        raise ARAXServiceError(
            f"ARAX service at {settings.arax_url} returned HTTP "
            f"{status_code}{body_head(response)}",
            status_code,
        )

    try:
        result = response.json()
    except Exception as e:
        # A 2xx whose body isn't TRAPI JSON is still a failed lookup, and
        # ARAX's status code is the most useful thing we know about it.
        raise ARAXServiceError(
            f"ARAX service at {settings.arax_url} returned HTTP {status_code} "
            f"with a body that could not be parsed as JSON: {e}",
            status_code,
        ) from e

    return add_shepherd_arax_to_edge_sources(result)


def error_response(message: dict, error: ARAXServiceError) -> dict:
    """A TRAPI response reporting a failed ARAX call.

    ``status``/``description`` are TRAPI Response fields, so the status code
    lands somewhere the caller already parses rather than only in the logs. The
    query graph is carried over and the result containers are emptied, so what
    comes back is still a valid TRAPI response for the query that was asked.
    """
    query_graph = {}
    if isinstance(message.get("message"), dict):
        query_graph = message["message"].get("query_graph") or {}
    return {
        "message": {
            "query_graph": query_graph,
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "results": [],
        },
        "status": "Error",
        "description": f"[HTTP {error.status_code}] {error}",
    }


async def arax(task, logger: logging.Logger):
    query_id = task[1]["query_id"]
    logger.info(f"Getting message from db for query id {query_id}")
    message = await get_message(query_id, logger)
    if is_pathfinder_query(message):
        task[1]["workflow"] = json.dumps([{"id": "arax.pathfinder"}])
    else:
        response_id = task[1]["response_id"]
        try:
            result = await call_arax(message, logger)
        except ARAXServiceError as e:
            # Leave the caller a TRAPI response that says what happened before
            # letting the failure reach run_task_lifecycle, which records it on
            # the span and routes the query to finish_query with an ERROR
            # status. Without this the response id still holds the echo of the
            # incoming query, so the caller gets a query that looks like it
            # simply found nothing.
            await save_message(response_id, error_response(message, e), logger)
            raise
        await save_message(response_id, result, logger)
        task[1]["workflow"] = json.dumps([{"id": "arax"}])


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, arax)


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, TASK_LIMIT
            ):
                asyncio.create_task(process_task(task, parent_ctx, logger, limiter))
        except asyncio.CancelledError:
            LOGGER.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
