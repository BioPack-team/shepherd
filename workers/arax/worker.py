"""ARAX entry module.

Runs ARAX's query pipeline in-process (DEC-14 in docs/ARAX_PORT_BASELINE.md):
the ported library in ``shepherd_utils/arax/`` interprets the query graph (or
the ARAXi operations / TRAPI workflow), runs the resulting plan -- Expand
against Retriever, overlays, filters, Resultify with ARAX's ranker, Infer,
Connect -- and applies the ResultTransformer, exactly as ARAX's own ``/query``
does. The worker used to proxy each query to a remote ARAX service instead.

TRAPI pathfinder queries (``query_graph.paths``) still go to the
``arax.pathfinder`` worker (DEC-7).
"""

import asyncio
import json
import logging
import time
import uuid

from opentelemetry.trace import get_current_span

from shepherd_utils.arax_progress import finish_progress, push_progress
from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.data_download import (
    ARAX_COHD,
    ARAX_CURIE_TO_PMIDS,
    ARAX_EXPLAINABLE_DTD,
    ARAX_FDA_APPROVED_DRUGS,
    ensure_arax_dbs,
    ensure_arax_pathfinder_dbs,
)
from shepherd_utils.db import get_message, get_message_sync, save_message_sync
from shepherd_utils.inject_shepherd_arax_provenance import (
    add_shepherd_arax_to_edge_sources,
)
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import get_tasks, run_task_lifecycle

# Queue name
STREAM = "arax"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)
# The data files ARAX's actions read (DEC-6). curie_ngd and the tier0 overlay
# sqlite (FET, Connect) are the pathfinder's, fetched by
# ensure_arax_pathfinder_dbs; autocomplete is only used by the UI API.
ARAX_WORKER_DBS = (
    ARAX_CURIE_TO_PMIDS,
    ARAX_EXPLAINABLE_DTD,
    ARAX_FDA_APPROVED_DRUGS,
    ARAX_COHD,
)
# Used when ARAX produced a response that can't be returned at all. ARAX's
# /query fails the same way (its child process dies serializing it).
INTERNAL_ERROR = 500


class ARAXServiceError(Exception):
    """ARAX did not produce a successful TRAPI response.

    Carries ARAX's HTTP status code (the one its ``/query`` would have answered
    with), so it reaches the task span as ``arax.status_code``, the query's logs
    via ``run_task_lifecycle``, and the caller.
    """

    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.status_code = status_code


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


def default_submitter() -> str:
    return "infores:shepherd-arax:{maturity}@{location}@{url}".format(
        maturity=settings.server_maturity,
        location=settings.server_location,
        url=settings.server_url,
    )


def run_arax(query: dict, response_id: str) -> tuple[dict, int]:
    """Answer ``query`` the way ARAX's non-streaming ``/query`` does.

    That is ``ARAXQuery().query_return_message(query)``, then ``to_dict()`` of
    the envelope with its ``http_status`` added (ARAX's
    ``_run_query_and_return_json_generator_nonstream``). The response is stored
    under ``response_id``, which becomes its ``envelope.id`` (DEC-3).

    Returns ``(response, http_status)``.
    """
    # Imported here, not at module level: the library is only needed in the
    # pool children that run queries.
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    if "submitter" not in query:
        query["submitter"] = default_submitter()
    envelope = ARAXQuery(response_id=response_id).query_return_message(query)
    envelope_dict = envelope.to_dict()
    http_status = getattr(envelope, "http_status", 200)
    envelope_dict["http_status"] = http_status
    return envelope_dict, http_status


def run_arax_stream(query: dict, response_id: str) -> tuple[dict, int]:
    """Answer a ``stream_progress`` query the way ARAX's streaming ``/query`` does.

    ARAX's ``query_return_stream`` yields NDJSON lines: log entries, the
    ``{pid, authorization}`` token, ``query_plan`` updates and heartbeats, and
    last the response envelope. Every line but the last is relayed to the
    server as it is yielded (``shepherd_utils.arax_progress``); the envelope is
    returned to be saved as the response.

    Returns ``(response, http_status)``. ARAX's stream itself is always HTTP
    200; the status returned is the one its non-streaming ``/query`` would have
    answered with, so the Shepherd query's outcome is the same either way.
    """
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    if "submitter" not in query:
        query["submitter"] = default_submitter()
    araxq = ARAXQuery(response_id=response_id)
    last = None
    for line in araxq.query_return_stream(query):
        if last is not None:
            push_progress(response_id, last)
        last = line
    if last is None:
        # ARAX's stream checks whether the query thread is already done before
        # its loop, so a query that finishes first (an input error, say)
        # streams nothing at all, not even the envelope. The query did run;
        # its envelope is the response, finished as the stream would have.
        response = araxq.response
        response.status = response.status.replace("DONE,", "")
        if response.envelope.status == "OK":
            response.envelope.status = "Success"
        return response.envelope.to_dict(), getattr(response, "http_status", 200)
    envelope = json.loads(last)
    return envelope, getattr(araxq.response, "http_status", 200)


def error_response(message: dict, error: ARAXServiceError) -> dict:
    """A TRAPI response reporting that ARAX could not return one.

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


def arax_query_task(query_id: str, response_id: str) -> dict:
    """Process-pool entrypoint: load the query, run ARAX, save the response.

    Only ids cross the process boundary; the query and the (potentially large)
    response are read and written in the child, which also keeps ARAX's work
    off the parent's event loop. Returns a small summary for the parent.

    ARAX's own error responses (validation errors, a failed action) are saved
    as ARAX returns them -- a TRAPI response with ARAX's status, description
    and log -- and reported with ARAX's HTTP status.
    """
    query = get_message_sync(query_id)
    if not query.get("stream_progress"):
        return _save_arax_response(query, response_id, *run_arax(query, response_id))
    try:
        return _save_arax_response(
            query, response_id, *run_arax_stream(query, response_id)
        )
    finally:
        # Also on failure, so a streaming client stops waiting
        finish_progress(response_id)


def _save_arax_response(
    query: dict, response_id: str, response: dict, http_status: int
) -> dict:
    """Save ARAX's response (or an error response) and summarize it."""
    try:
        # ARAX serializes with the stdlib json and allow_nan=False, and fails
        # the request on NaN. Saving what that produces also turns the numpy
        # floats some actions put in attributes (e.g. Infer's pandas scores;
        # json writes them as floats) into plain floats, which Shepherd's
        # store (orjson) would otherwise reject.
        response = json.loads(json.dumps(response, allow_nan=False))
    except (ValueError, TypeError) as e:
        error = ARAXServiceError(
            f"ARAX's response could not be serialized to JSON: {e}", INTERNAL_ERROR
        )
        save_message_sync(response_id, error_response(query, error))
        return {"http_status": INTERNAL_ERROR, "error": str(error)}
    if 200 <= http_status < 300:
        response = add_shepherd_arax_to_edge_sources(response)
    save_message_sync(response_id, response)
    return {
        "http_status": http_status,
        "status": response.get("status"),
        "description": response.get("description"),
        "n_results": len((response.get("message") or {}).get("results") or []),
    }


async def arax(task, logger: logging.Logger, loop=None, pool=None):
    query_id = task[1]["query_id"]
    logger.info(f"Getting message from db for query id {query_id}")
    message = await get_message(query_id, logger)
    if is_pathfinder_query(message):
        task[1]["workflow"] = json.dumps([{"id": "arax.pathfinder"}])
        return
    response_id = task[1]["response_id"]
    loop = loop or asyncio.get_running_loop()
    if pool is None:
        summary = await loop.run_in_executor(
            None, arax_query_task, query_id, response_id
        )
    else:
        summary = await pool.run(loop, arax_query_task, query_id, response_id)
    http_status = summary["http_status"]
    get_current_span().set_attribute("arax.status_code", http_status)
    if "error" in summary:
        raise ARAXServiceError(summary["error"], http_status)
    logger.info(
        f"ARAX finished with HTTP {http_status}, status {summary['status']}: "
        f"{summary['description']} ({summary['n_results']} results)"
    )
    if not 200 <= http_status < 300:
        # Let run_task_lifecycle record it and route the query to finish_query
        # with an ERROR status; ARAX's own response (with its log) is saved.
        raise ARAXServiceError(
            f"ARAX returned HTTP {http_status} ({summary['status']}): "
            f"{summary['description']}",
            http_status,
        )
    task[1]["workflow"] = json.dumps([{"id": "arax"}])


async def process_task(task, parent_ctx, logger: logging.Logger, limiter, loop, pool):
    """Process a given task and ACK in redis."""

    async def _run(task, logger):
        await arax(task, logger, loop, pool)

    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, _run)


def warm_biolink_cache(logger: logging.Logger) -> None:
    """Build ARAX's Biolink lookup map before the first query does.

    BiolinkHelper caches the map in ``settings.arax_biolink_cache_dir``
    (downloading the Biolink model YAML on a cold cache) and builds it on first
    use, which otherwise lands on the first query after each container start.
    The pool children read the same cached files. A failure only logs: the
    first query then builds it, as before.
    """
    started = time.monotonic()
    try:
        from shepherd_utils.arax.BiolinkHelper.biolink_helper import (
            get_biolink_helper,
        )

        get_biolink_helper()
    except Exception as e:
        logger.warning(
            f"Could not warm the Biolink cache ({type(e).__name__}: {e}); "
            "the first query will build it"
        )
        return
    logger.info(
        f"Biolink lookup map ready in {settings.arax_biolink_cache_dir} "
        f"({time.monotonic() - started:.1f}s)"
    )


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    # First run: fetch the data files ARAX's actions read (DEC-6). No-ops once
    # they are on the mounted volumes.
    ensure_arax_pathfinder_dbs(LOGGER)
    ensure_arax_dbs(ARAX_WORKER_DBS, LOGGER)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, warm_biolink_cache, LOGGER)
    # ARAX's plan is CPU-heavy (Resultify, the ranker, overlays) and starts its
    # own event loops (Expand, FET), so each query runs in a pool child. Size the
    # pool by the pod's CPU allocation; POOL_MAX_WORKERS overrides.
    max_workers = resolve_pool_workers(TASK_LIMIT, LOGGER)
    LOGGER.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="arax process pool",
        task_timeout=settings.pool_task_timeout_sec,
    )
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, max_workers
            ):
                asyncio.create_task(
                    process_task(task, parent_ctx, logger, limiter, loop, pool)
                )
        except asyncio.CancelledError:
            LOGGER.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
