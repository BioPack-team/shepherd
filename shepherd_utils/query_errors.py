"""Worker failures that say how the query should be reported.

A worker that fails with a plain exception ends its query ``ERROR``, which
``/query`` answers as a 500: the operation broke. Some failures aren't that.
BTE refusing a Pathfinder query is a capability the ARA doesn't have, which
TRAPI spells 501 Not Implemented. The ARA's own worker is the only place that
knows which one happened, so it raises a ``QueryError`` carrying the HTTP code
and a client-safe description, and the code travels with the query from there:

    worker raises QueryError(code, description)
      -> run_task_lifecycle -> handle_task_failure  (finish_query task fields)
      -> finish_query -> shepherd_brain.status / .description
      -> /query (and /arax/query) answer with the code
      -> /asyncquery_status reports the description
      -> an ARS child becomes E/<code> instead of D/200

The code rides in the stored status itself (``"ERROR 501"``) instead of a new
column, so nothing about the schema changes. A bare ``"ERROR"`` still means
500, so every query that failed the old way reads exactly as before.
"""

from typing import Optional

ERROR_STATUS = "ERROR"
DEFAULT_ERROR_CODE = 500
# Codes a worker may hand back. Outside this range a code is ignored and the
# query fails as a plain 500: a worker must not be able to make a failed query
# answer 200, or 3xx/1xx that callers would misread.
_MIN_CODE, _MAX_CODE = 400, 599


def valid_error_code(code) -> Optional[int]:
    """``code`` as an int when it is a usable error code, else None."""
    if isinstance(code, bool):
        return None
    try:
        code = int(code)
    except (TypeError, ValueError):
        return None
    return code if _MIN_CODE <= code <= _MAX_CODE else None


class QueryError(Exception):
    """A worker failure that names the HTTP code the query should end with.

    ``description`` is returned to the caller verbatim (the TRAPI response's
    ``description``, the async status, the ARS child's log), so it must be
    client-safe: say what the ARA can't do, not how its internals broke.
    """

    status_code = DEFAULT_ERROR_CODE

    def __init__(self, description: str, status_code: Optional[int] = None):
        super().__init__(description)
        self.description = description
        if status_code is not None:
            self.status_code = valid_error_code(status_code) or DEFAULT_ERROR_CODE


class UnsupportedQueryError(QueryError):
    """The ARA does not implement this kind of query (TRAPI's 501)."""

    status_code = 501


def error_status(code: Optional[int] = None) -> str:
    """The stored status for a query failed with ``code``.

    A plain failure (no code, or the default 500) keeps the bare ``"ERROR"``
    every existing reader expects.
    """
    code = valid_error_code(code)
    if code is None or code == DEFAULT_ERROR_CODE:
        return ERROR_STATUS
    return f"{ERROR_STATUS} {code}"


def status_error_code(status: Optional[str]) -> Optional[int]:
    """The HTTP code a stored status carries (``"ERROR 501"`` -> 501), or None
    when it carries none (``"ERROR"``, ``"OK"``, ``"TIMEOUT"``, ...)."""
    if not isinstance(status, str):
        return None
    prefix = f"{ERROR_STATUS} "
    if not status.startswith(prefix):
        return None
    return valid_error_code(status[len(prefix) :].strip())


def is_error_status(status: Optional[str]) -> bool:
    """Whether a stored status is a failed operation (``ERROR`` or ``ERROR <code>``)."""
    return status == ERROR_STATUS or status_error_code(status) is not None
