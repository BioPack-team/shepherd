"""Whole-query timeout budget carried alongside every task.

The ARS and the other external callers give up on a Shepherd query after about
five minutes, and the synchronous ``/query`` endpoint stops holding its
connection open around the same point. Work done after that is work nobody is
waiting for -- but the pipeline had no notion of it: a task handed from worker
to worker kept going indefinitely, each hop taking a concurrency slot (and, for
the CPU-bound workers, a process-pool child) away from queries whose answer can
still be delivered, while the query's row sat in a non-terminal state until the
monitor's abandoned-query reaper eventually swept it.

So each query now carries an absolute deadline, stamped once at intake and
passed along with the task from operation to operation. A worker checks it as
it picks the task up; if the budget is spent it hands the query to
``finish_query`` instead of running the operation (see
``shepherd_utils.shared``), so the query ends the ordinary way -- terminal state
in Postgres, callback rows reaped, logs saved, whatever was gathered POSTed to
the callback URL -- rather than being dropped on the floor.

The deadline is stored as absolute epoch seconds rather than a start time plus
a budget: one field per task, and every stage agrees on the same instant without
needing to know how the budget was chosen. Comparing it against the local clock
assumes containers agree on the time to within a second or so, which is true of
NTP-synced hosts and, for the common case, is the same clock anyway.
"""

import time
from typing import Any, Dict, Mapping, Optional

from .config import settings

# Task payload field holding the query's absolute deadline (epoch seconds).
DEADLINE_FIELD = "query_deadline"

# ``status`` recorded in ``shepherd_brain`` for a query cut short by its budget.
# Deliberately distinct from the ERROR a failed operation records: nothing went
# wrong here, there just wasn't time left to keep going.
TIMEOUT_STATUS = "TIMEOUT"


def query_budget(query: Optional[Mapping[str, Any]] = None) -> float:
    """How many seconds of work a query gets before it stops being useful.

    ``settings.query_timeout_sec`` is the fleet-wide budget, matching what
    upstream callers wait. A client that explicitly asks to wait *longer* (TRAPI
    ``parameters.timeout``, which the sync endpoint also polls against) is not
    cut short: its own number wins when it is the larger of the two. An
    unparseable value is ignored rather than failing the query. Returns 0 when
    the budget is disabled, meaning "no deadline".
    """
    budget = float(settings.query_timeout_sec)
    if budget <= 0:
        return 0.0
    parameters = (query or {}).get("parameters") or {}
    requested = parameters.get("timeout") if isinstance(parameters, Mapping) else None
    if requested is not None:
        try:
            budget = max(budget, float(requested))
        except (TypeError, ValueError):
            pass
    return budget


def query_deadline(
    query: Optional[Mapping[str, Any]] = None,
    start: Optional[float] = None,
) -> Optional[float]:
    """Absolute epoch time by which this query's work must be finished.

    ``None`` when the budget is disabled, which leaves the task unstamped and
    therefore never expired.
    """
    budget = query_budget(query)
    if budget <= 0:
        return None
    return (time.time() if start is None else start) + budget


def deadline_field(deadline: Optional[float]) -> Dict[str, str]:
    """Payload fragment carrying ``deadline``, or nothing when there isn't one.

    Millisecond precision is far finer than anything that depends on it, and
    keeps the stream entry short.
    """
    if deadline is None:
        return {}
    return {DEADLINE_FIELD: f"{float(deadline):.3f}"}


def carry_deadline(fields: Any) -> Dict[str, str]:
    """Payload fragment propagating a task's deadline to its follow-on task.

    Every hop that enqueues the next operation passes this through, so the
    budget is measured from the query's intake rather than restarting at each
    stage.
    """
    return deadline_field(get_deadline(fields))


def get_deadline(fields: Any) -> Optional[float]:
    """Read the deadline off a task payload, or ``None`` if it hasn't got one."""
    if not hasattr(fields, "get"):
        return None
    raw = fields.get(DEADLINE_FIELD)
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def seconds_overdue(fields: Any, now: Optional[float] = None) -> float:
    """How far past its deadline this task's query is; 0.0 if it isn't.

    Fails open: a task with no deadline -- one enqueued by an older server, or
    by a path that doesn't stamp one -- is never overdue and runs exactly as it
    did before.
    """
    deadline = get_deadline(fields)
    if deadline is None:
        return 0.0
    return max(0.0, (time.time() if now is None else now) - deadline)


def is_expired(fields: Any, now: Optional[float] = None) -> bool:
    """Whether this task's query has outlived its budget."""
    return seconds_overdue(fields, now) > 0.0
