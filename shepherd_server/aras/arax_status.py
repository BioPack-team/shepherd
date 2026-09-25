"""ARAX's ``GET /status`` (API-09) and its query tracker views (OPS-01..03), for
Shepherd's ARAX API.

ARAX keeps its own query table in MySQL; here the views are built from
Shepherd's query table (``shepherd_brain``, filtered to queries routed to
ARAX), in the shape ARAX's ``ARAXQueryTracker`` returns them.

``terminate_pid`` (OPS-02): ARAX SIGTERMs the forked child running the query,
and the client sees its stream end. Shepherd runs the query in a worker's pool
child in another container, which it cannot signal (and killing a pool child
would break the pool). So the server hands out its own token in place of
ARAX's ``{pid, authorization}`` line as it relays the stream, and terminating
ends that query's stream, which is what the client of ARAX's observes. The
query itself runs to completion in the worker, and its response is still
stored.
"""

import json
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from shepherd_utils.config import settings
from shepherd_utils.db import data_db_client, get_message, get_recent_queries

LOGGER = logging.getLogger("shepherd.arax.status")
TOKEN_TTL_SEC = 24 * 3600
TOKEN_COUNTER_KEY = "arax_pid_counter"

# shepherd_brain.state -> ARAX's tracker states
ARAX_STATES = {"QUEUED": "started", "COMPLETED": "Completed", "ABANDONED": "Died"}


def _token_key(pid) -> str:
    return f"arax_pid:{pid}"


def _terminated_key(response_id: str) -> str:
    return f"arax_terminated:{response_id}"


def pid_authorization(pid) -> str:
    """ARAX's kill-token authorization for a pid (``hash('Pickles' + pid)``)."""
    return str(hash("Pickles" + str(pid)))


async def issue_pid_token(response_id: str) -> dict:
    """A ``{pid, authorization}`` token for a streamed query, unique across the
    deployment, that ``terminate`` can later resolve to the query."""
    pid = int(await data_db_client.incr(TOKEN_COUNTER_KEY))
    authorization = pid_authorization(pid)
    await data_db_client.set(
        _token_key(pid),
        json.dumps({"response_id": response_id, "authorization": authorization}),
        ex=TOKEN_TTL_SEC,
    )
    return {"pid": pid, "authorization": authorization}


async def is_terminated(response_id: str) -> bool:
    return bool(await data_db_client.exists(_terminated_key(response_id)))


async def terminate(terminate_pid, authorization) -> dict:
    """ARAXQueryTracker.terminate_job, with the token resolved to a stream."""
    raw = await data_db_client.get(_token_key(terminate_pid))
    token = json.loads(raw) if raw is not None else None
    reference_authorization = (
        token["authorization"] if token else pid_authorization(terminate_pid)
    )
    if authorization is None or str(authorization) != reference_authorization:
        return {"status": "ERROR", "description": "Invalid authorization provided"}
    if token is None:
        return {
            "status": "ERROR",
            "description": f"ERROR: Attempt to terminate pid={terminate_pid} failed",
        }
    await data_db_client.set(
        _terminated_key(token["response_id"]), "1", ex=TOKEN_TTL_SEC
    )
    return {"status": "OK", "description": f"Process {terminate_pid} terminated"}


def _format_datetime(value: Optional[datetime]) -> Optional[str]:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value is not None else None


def _elapsed(start: Optional[datetime], stop: Optional[datetime]) -> Optional[int]:
    if start is None:
        return None
    end = stop if stop is not None else datetime.now(start.tzinfo)
    return max(int((end - start).total_seconds()), 0)


def tracker_entry(row: tuple) -> dict:
    """One ``recent_queries`` entry, from a shepherd_brain row."""
    (
        qid,
        start,
        stop,
        submitter,
        remote_ip,
        _domain,
        hostname,
        response_id,
        _cb,
        state,
        status,
        description,
    ) = row[:12]
    return {
        "query_id": qid,
        "pid": None,
        "start_datetime": _format_datetime(start),
        "domain": urlparse(settings.server_url).netloc,
        "hostname": hostname,
        "instance_name": settings.server_location,
        "state": ARAX_STATES.get(state, state),
        "elapsed": _elapsed(start, stop),
        "submitter": submitter,
        "response_id": response_id,
        "status": status,
        "description": description,
        "remote_address": remote_ip,
    }


async def recent_queries(last_n_hours=None, mode=None) -> dict:
    """ARAXQueryTracker.get_status: the recent (or, with mode=active, ongoing) queries."""
    if last_n_hours is None or last_n_hours == 0:
        last_n_hours = 24
    rows = await get_recent_queries(
        "arax", float(last_n_hours), mode == "active", LOGGER
    )
    result = {"recent_queries": [tracker_entry(row) for row in rows]}
    result["recent_queries"].reverse()
    result["current_datetime"] = datetime.now().strftime("%Y-%m-%d %T")
    return result


async def query_by_id(id_):
    """ARAXQueryTracker.get_query_by_id: the stored input query, or None."""
    return await get_message(str(id_), LOGGER)


def kp_cache_listing() -> dict:
    """KPQueryCacher.list_cached_queries: ARAX's KP cache, in Shepherd's data
    store (DEC-18). Blocking; call it off the event loop."""
    from shepherd_utils.arax.Expand.trapi_query_cacher import KPQueryCacher

    return KPQueryCacher().list_cached_queries()
