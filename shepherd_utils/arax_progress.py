"""Relay of ARAX's streamed query progress from the arax worker to the server.

ARAX's ``/query`` with ``stream_progress: true`` streams NDJSON: its log
entries as they happen, a ``{pid, authorization}`` token, ``query_plan``
updates, heartbeats, and finally the response envelope (API-02). The ARAX UI
draws its progress display from that stream.

In Shepherd the query runs in the arax worker, so the worker pushes each line
ARAX's own ``query_return_stream`` yields (all but the final envelope, which is
saved as the query's response) onto a Redis list keyed by the response id, and
then a done marker. The server's streaming endpoint reads the list as it grows
and relays the lines to the client, then sends the saved response.
"""

from typing import List, Optional

from shepherd_utils.db import _get_sync_data_db, data_db_client

# How long the relayed lines are kept after they are written. They are only
# needed while the client is streaming, but a client may attach late.
PROGRESS_TTL_SEC = 24 * 3600
# Pushed after the last line (and after the response is saved)
DONE_MARKER = "__arax_progress_done__"


def progress_key(response_id: str) -> str:
    return f"arax_progress:{response_id}"


def push_progress(response_id: str, line: str) -> None:
    """Append one streamed line (sync; runs in the worker's pool child)."""
    client = _get_sync_data_db()
    key = progress_key(response_id)
    pipe = client.pipeline()
    pipe.rpush(key, line)
    pipe.expire(key, PROGRESS_TTL_SEC)
    pipe.execute()


def finish_progress(response_id: str) -> None:
    """Mark the stream complete: every line is pushed and the response saved."""
    push_progress(response_id, DONE_MARKER)


async def read_progress(response_id: str, start: int) -> List[str]:
    """The lines pushed so far from index ``start`` on (the done marker included)."""
    items = await data_db_client.lrange(progress_key(response_id), start, -1)
    return [i.decode("utf-8") if isinstance(i, bytes) else i for i in items]


def is_done(line: Optional[str]) -> bool:
    return line == DONE_MARKER
