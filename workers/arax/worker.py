"""ARAX entry module.

Runs ARAX's query pipeline in-process (DEC-14 in docs/ARAX_PORT_BASELINE.md):
the ported library in ``shepherd_utils/arax/`` interprets the query graph (or
the ARAXi operations / TRAPI workflow), runs the resulting plan -- Expand
against Retriever, overlays, filters, Resultify with ARAX's ranker, Infer,
Connect -- and applies the ResultTransformer, exactly as ARAX's own ``/query``
does. The worker used to proxy each query to a remote ARAX service instead.

TRAPI pathfinder queries (``query_graph.paths``) still go to the
``arax.pathfinder`` worker (DEC-7).

Tracing: the query runs in a pool child, which continues the task's trace
from a carrier the parent injects (``setup_pool_child_tracer``). The child's
``arax.query`` span holds ``arax.load_query``, ARAX's own steps -- one span
per ARAXi action, per KP Expand queried, and the HTTP calls beneath them
(``shepherd_utils.arax_tracing``) -- and ``arax.save_response``.
"""

import asyncio
import json
import logging
import time
import uuid

from opentelemetry.propagate import extract, inject
from opentelemetry.trace import Status, StatusCode, get_current_span

from shepherd_utils.arax_progress import finish_progress, push_progress
from shepherd_utils.arax_tracing import instrument_arax
from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.data_download import (
    ARAX_COHD,
    ARAX_CURIE_TO_PMIDS,
    ARAX_EXPLAINABLE_DTD,
    ARAX_FDA_APPROVED_DRUGS,
    arax_biolink_cache_path,
    ensure_arax_dbs,
    ensure_arax_pathfinder_dbs,
)
from shepherd_utils.db import (
    _get_sync_data_db,
    get_message,
    get_message_sync,
    save_logs,
    save_response_sync,
)
from shepherd_utils.inject_shepherd_arax_provenance import (
    add_shepherd_arax_to_edge_sources,
)
from shepherd_utils.logger import get_query_handler, get_worker_logger
from shepherd_utils.otel import setup_pool_child_tracer, setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import get_tasks, run_task_lifecycle
from shepherd_utils.trapi import normalize_query_graph

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
    comes back is still a valid TRAPI 2.0 response for the query that was asked
    (no query graph at all rather than an invalid empty one).
    """
    response_message = {
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "results": [],
    }
    query_graph = None
    if isinstance(message.get("message"), dict):
        query_graph = message["message"].get("query_graph")
    if isinstance(query_graph, dict) and query_graph:
        normalize_query_graph(query_graph)
        response_message = {"query_graph": query_graph, **response_message}
    return {
        "message": response_message,
        "status": "Error",
        "description": f"[HTTP {error.status_code}] {error}",
    }


def arax_log_entries(response: dict) -> list:
    """Take ARAX's log off its response, as TRAPI 2.0 LogEntries.

    A stored response carries no ``logs`` (``save_response``): a query's logs
    live in Shepherd's log store and are added when the response is delivered.
    So ARAX's log -- part of its response, and what ARAX's UI shows -- goes
    there too, in ARAX's order. Null members (ARAX's ``code: None``) are
    dropped; 2.0 has no nulls.
    """
    logs = response.pop("logs", None) or []
    return [
        {k: v for k, v in entry.items() if v is not None}
        for entry in logs
        if isinstance(entry, dict)
    ]


def arax_query_task(
    query_id: str,
    response_id: str,
    otel_carrier: dict | None = None,
    submitted_at: float | None = None,
) -> dict:
    """Process-pool entrypoint: load the query, run ARAX, save the response.

    Only ids cross the process boundary; the query and the (potentially large)
    response are read and written in the child, which also keeps ARAX's work
    off the parent's event loop. Returns a small summary for the parent.

    ARAX's own error responses (validation errors, a failed action) are saved
    as ARAX returns them -- a TRAPI response with ARAX's status, description
    and log -- and reported with ARAX's HTTP status.

    The response is saved in Shepherd's stored form (``save_response_sync``:
    no delivery envelope, pruned to 2.0's no-null / no-empty rules); ARAX's log
    comes back in the summary (``logs``) for the parent to put in the query's
    log store.

    ``otel_carrier`` is the parent's span context: the query's spans start
    under it. ``submitted_at`` (``time.time()`` in the parent) gives the time
    the task waited for a free pool child.
    """
    setup_pool_child_tracer(STREAM)
    instrument_arax(http_clients=settings.otel_enabled)
    parent_ctx = extract(otel_carrier) if otel_carrier else None
    with tracer.start_as_current_span("arax.query", context=parent_ctx) as span:
        span.set_attribute("arax.response_id", response_id)
        if submitted_at is not None:
            span.set_attribute(
                "arax.pool.wait_ms", max(0.0, (time.time() - submitted_at) * 1000)
            )
        with tracer.start_as_current_span("arax.load_query"):
            query = get_message_sync(query_id)
        _set_query_attributes(span, query)
        if not query.get("stream_progress"):
            summary = _save_arax_response(
                query, response_id, *run_arax(query, response_id)
            )
        else:
            try:
                summary = _save_arax_response(
                    query, response_id, *run_arax_stream(query, response_id)
                )
            finally:
                # Also on failure, so a streaming client stops waiting
                finish_progress(response_id)
        _set_summary_attributes(span, summary)
        return summary


def _set_query_attributes(span, query: dict) -> None:
    """What was asked: the query graph's shape, or the ARAXi / workflow plan."""
    span.set_attribute("arax.stream_progress", bool(query.get("stream_progress")))
    message = query.get("message")
    query_graph = message.get("query_graph") if isinstance(message, dict) else None
    if isinstance(query_graph, dict):
        span.set_attribute("arax.qgraph.nodes", len(query_graph.get("nodes") or {}))
        span.set_attribute("arax.qgraph.edges", len(query_graph.get("edges") or {}))
        knowledge_types = sorted(
            {
                str(qedge.get("knowledge_type") or "lookup")
                for qedge in (query_graph.get("edges") or {}).values()
                if isinstance(qedge, dict)
            }
        )
        if knowledge_types:
            span.set_attribute("arax.qgraph.knowledge_types", knowledge_types)
    operations = query.get("operations")
    if isinstance(operations, dict) and operations.get("actions"):
        span.set_attribute("arax.araxi.n_commands", len(operations["actions"]))
    workflow = query.get("workflow")
    if isinstance(workflow, list) and workflow:
        span.set_attribute(
            "arax.workflow",
            [str(op.get("id")) for op in workflow if isinstance(op, dict)],
        )
    if query.get("submitter"):
        span.set_attribute("arax.submitter", str(query["submitter"]))


def _set_summary_attributes(span, summary: dict) -> None:
    """How it went: ARAX's status and the size of what was saved."""
    http_status = summary["http_status"]
    span.set_attribute("arax.status_code", http_status)
    for key in ("status", "n_results"):
        if summary.get(key) is not None:
            span.set_attribute(f"arax.{key}", summary[key])
    if not 200 <= http_status < 300:
        span.set_status(
            Status(
                StatusCode.ERROR,
                str(summary.get("error") or summary.get("description") or ""),
            )
        )


def _save_arax_response(
    query: dict, response_id: str, response: dict, http_status: int
) -> dict:
    """Save ARAX's response (or an error response) and summarize it."""
    with tracer.start_as_current_span("arax.save_response") as span:
        summary = _save_and_summarize(query, response_id, response, http_status)
        for key in ("n_results", "n_kg_nodes", "n_kg_edges", "response_bytes"):
            if summary.get(key) is not None:
                span.set_attribute(f"arax.{key}", summary[key])
        return summary


def _save_and_summarize(
    query: dict, response_id: str, response: dict, http_status: int
) -> dict:
    try:
        # ARAX serializes with the stdlib json and allow_nan=False, and fails
        # the request on NaN. Saving what that produces also turns the numpy
        # floats some actions put in attributes (e.g. Infer's pandas scores;
        # json writes them as floats) into plain floats, which Shepherd's
        # store (orjson) would otherwise reject.
        serialized = json.dumps(response, allow_nan=False)
        response = json.loads(serialized)
    except (ValueError, TypeError) as e:
        error = ARAXServiceError(
            f"ARAX's response could not be serialized to JSON: {e}", INTERNAL_ERROR
        )
        save_response_sync(response_id, error_response(query, error))
        return {"http_status": INTERNAL_ERROR, "error": str(error), "logs": []}
    logs = arax_log_entries(response)
    if 200 <= http_status < 300:
        response = add_shepherd_arax_to_edge_sources(response)
    save_response_sync(response_id, response)
    response_message = response.get("message") or {}
    knowledge_graph = response_message.get("knowledge_graph") or {}
    return {
        "http_status": http_status,
        "status": response.get("status"),
        "description": response.get("description"),
        "n_results": len(response_message.get("results") or []),
        "n_kg_nodes": len(knowledge_graph.get("nodes") or {}),
        "n_kg_edges": len(knowledge_graph.get("edges") or {}),
        "response_bytes": len(serialized),
        "logs": logs,
    }


async def save_arax_logs(
    response_id: str, entries: list, logger: logging.Logger
) -> None:
    """Put ARAX's log entries in the query's log store, now.

    They join the task logger's queue (as a pool child's records do) and are
    flushed straight away rather than when the task wraps up: the next
    operation is queued before that flush, and the response is delivered with
    whatever the store holds by then.
    """
    handler = get_query_handler(logger)
    if not entries or handler is None:
        return
    handler.ingest(entries)
    await save_logs(response_id, logger)


async def arax(task, logger: logging.Logger, loop=None, pool=None):
    query_id = task[1]["query_id"]
    logger.info(f"Getting message from db for query id {query_id}")
    message = await get_message(query_id, logger)
    span = get_current_span()
    if is_pathfinder_query(message):
        span.set_attribute("arax.routed_to", "arax.pathfinder")
        task[1]["workflow"] = json.dumps([{"id": "arax.pathfinder"}])
        return
    response_id = task[1]["response_id"]
    loop = loop or asyncio.get_running_loop()
    # The pool child continues this trace under the current task span
    carrier: dict = {}
    inject(carrier)
    args = (query_id, response_id, carrier, time.time())
    if pool is None:
        summary = await loop.run_in_executor(None, arax_query_task, *args)
    else:
        summary = await pool.run(loop, arax_query_task, *args)
    http_status = summary["http_status"]
    span.set_attribute("arax.status_code", http_status)
    if summary.get("n_results") is not None:
        span.set_attribute("arax.n_results", summary["n_results"])
    await save_arax_logs(response_id, summary.get("logs") or [], logger)
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

    BiolinkHelper caches the map in ``arax_biolink_cache_path()`` (on the
    mounted ``arax_dbs`` volume by default, so it survives restarts)
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
        f"Biolink lookup map ready in {arax_biolink_cache_path()} "
        f"({time.monotonic() - started:.1f}s)"
    )


# One KP-cache refresh pass runs at a time across every arax worker replica
KP_CACHE_REFRESH_LOCK_KEY = "arax_kp_cache:refresh_lock"
# A pass stops starting queries after 60 s (REFRESH_TIME_LIMIT_SECONDS), and each
# refresh query times out after 30 s
KP_CACHE_REFRESH_LOCK_TTL_SEC = 120
BACKGROUND_TASKS: set = set()


def refresh_kp_cache_once(logger: logging.Logger) -> bool:
    """One pass of ARAX's KP-cache refresh (KPQueryCacher.refresh_cache), unless
    another replica is running one. Returns whether this call ran it."""
    from shepherd_utils.arax.Expand.trapi_query_cacher import KPQueryCacher

    db = _get_sync_data_db()
    token = uuid.uuid4().hex
    if not db.set(
        KP_CACHE_REFRESH_LOCK_KEY, token, nx=True, ex=KP_CACHE_REFRESH_LOCK_TTL_SEC
    ):
        return False
    try:
        KPQueryCacher().refresh_cache()
    except Exception as e:
        logger.warning(f"KP cache refresh failed: {type(e).__name__}: {e}")
    finally:
        if db.get(KP_CACHE_REFRESH_LOCK_KEY) == token.encode():
            db.delete(KP_CACHE_REFRESH_LOCK_KEY)
    return True


async def kp_cache_refresh_loop(loop, logger: logging.Logger) -> None:
    """ARAX's background tasker's KP-cache refresh: a pass every
    ``arax_kp_cache_refresh_interval_sec`` (a minute by default, as in ARAX)."""
    interval = settings.arax_kp_cache_refresh_interval_sec
    if not settings.arax_kp_cache_enabled or interval <= 0:
        return
    while True:
        started = time.monotonic()
        try:
            await loop.run_in_executor(None, refresh_kp_cache_once, logger)
        except Exception as e:
            logger.warning(f"KP cache refresh failed: {type(e).__name__}: {e}")
        await asyncio.sleep(max(1.0, interval - (time.monotonic() - started)))


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    # First run: fetch the data files ARAX's actions read (DEC-6). No-ops once
    # they are on the mounted volumes.
    ensure_arax_pathfinder_dbs(LOGGER)
    ensure_arax_dbs(ARAX_WORKER_DBS, LOGGER)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, warm_biolink_cache, LOGGER)
    # held so the task isn't garbage-collected
    BACKGROUND_TASKS.add(asyncio.create_task(kp_cache_refresh_loop(loop, LOGGER)))
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
