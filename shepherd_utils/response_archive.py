"""The archive of finished ARA queries in the cold store.

Redis holds a query's response while the query runs. When the pipeline
finishes, ``finish_query`` keeps a copy here -- the response as the pipeline
left it, before anything downstream (a callback envelope, the ARS's
premerge) touches it. That copy is what stays readable for
``ara_response_retention_days`` after the Redis copy has expired, for a
query submitted directly and for one the ARS dispatched alike.

One object per query, self-contained. It is three zstd frames back to back,
which decompress to three JSON documents in a row:

1. the response, in its stored form (no delivery envelope; see
   ``shepherd_utils.trapi.prepare_stored_response``) -- the very frame Redis
   held;
2. the query -- again the frame Redis held, or ``null`` if it had already
   expired. It is what carries the ``parameters`` a served response repeats;
3. a tail written here: how the query ended (``status``, ``description``)
   and its ``logs``.

The first two frames are copied as compressed bytes and never decompressed
or parsed, so archiving costs ``finish_query`` -- where memory is what limits
it -- only the compressed size of the response, about a thirtieth of the
decoded one. Taking the object apart and wrapping it as a TRAPI Response is
the reader's job (``load_archive``); no client ever sees the three parts.

``finish_query`` does it in two steps: ``capture_response`` reads the
response's compressed bytes before it hands the response to anyone, and
``archive_query`` writes the object once the query is delivered and its
last logs are flushed, so the archived logs are complete.

The archive is a debugging record, never part of delivering a query: every
step here is best-effort. A failure is retried, bounded in time, then
logged, and the query is delivered regardless. Nothing is archived (and
every read answers None) unless a store is configured
(``shepherd_utils/ars/payload_store.py``).
"""

import asyncio
import logging
from typing import Any, List, NamedTuple, Optional

import orjson
import zstandard

import shepherd_utils.db as shepherd_db
from shepherd_utils.ars import payload_store

ARCHIVE_ATTEMPTS = 3
# The most the archive write may hold up the worker finishing a query,
# retries included: a store that hangs must cost the copy, not a task slot.
ARCHIVE_TIMEOUT_SEC = 30.0
# Frames in an archived object: response, query, tail.
FRAMES = 3
ZSTD_MAGIC = zstandard.FRAME_HEADER
_NULL = b"null"


class Archive(NamedTuple):
    """An archived query, taken apart."""

    response: bytes  # the stored response, as decompressed JSON (never parsed)
    query: Optional[dict]  # None when it had expired before the archive
    status: Optional[str]
    description: Optional[str]
    logs: List[dict]


def _is_archivable(message_id: Any) -> bool:
    """Only UUID ids have a place in the store (``relative_key``); anything
    else -- an id from a caller's URL, say -- simply has no archive."""
    try:
        payload_store.relative_key(message_id)
    except (ValueError, AttributeError, TypeError):
        return False
    return True


async def capture_response(response_id: str, logger: logging.Logger) -> Optional[bytes]:
    """The finished response's compressed bytes, to archive once the query
    is delivered (``archive_query``).

    Read before the response is handed to anyone, so what is archived is
    what the pipeline produced. None when there is nothing to archive: no
    store configured, an id with no place in it, or no response in Redis.
    Never raises.
    """
    try:
        if not payload_store.enabled() or not _is_archivable(response_id):
            return None
        return await shepherd_db.data_db_client.get(response_id)
    except Exception as e:
        logger.warning(f"Could not read response {response_id} to archive it: {e}")
        return None


def _tail_frame(
    status: Optional[str], description: Optional[str], log_entries: List[bytes]
) -> bytes:
    """The third frame. Each log entry is already one JSON document, so the
    list is assembled from them as they are."""
    head = orjson.dumps({"status": status, "description": description})
    logs = b"[" + b",".join(bytes(entry) for entry in log_entries) + b"]"
    return zstandard.compress(head[:-1] + b',"logs":' + logs + b"}")


async def _build(
    response_blob: bytes,
    query_id: str,
    response_id: str,
    status: Optional[str],
    description: Optional[str],
    logger: logging.Logger,
) -> bytes:
    """The archived object: the response frame, the query frame, the tail."""
    query_blob = None
    try:
        query_blob = await shepherd_db.data_db_client.get(query_id)
    except Exception as e:
        logger.warning(f"Archiving {response_id} without its query: {e}")
    if not query_blob or bytes(query_blob[:4]) != ZSTD_MAGIC:
        query_blob = zstandard.compress(_NULL)
    log_entries: List[bytes] = []
    try:
        log_entries = await shepherd_db.logs_db_client.lrange(response_id, 0, -1)
    except Exception as e:
        logger.warning(f"Archiving {response_id} without its logs: {e}")
    tail = await asyncio.to_thread(_tail_frame, status, description, log_entries)
    return b"".join((bytes(response_blob), bytes(query_blob), tail))


async def _write_with_retries(
    response_id: str, blob: bytes, logger: logging.Logger
) -> bool:
    last_error: Optional[Exception] = None
    for attempt in range(ARCHIVE_ATTEMPTS):
        if attempt:
            await asyncio.sleep(0.1 * (2**attempt))
        try:
            await asyncio.to_thread(
                payload_store.write, response_id, blob, payload_store.ARA
            )
            return True
        except Exception as e:
            last_error = e
            logger.warning(f"Failed to archive {response_id} (attempt {attempt}): {e}")
    logger.error(
        f"Could not archive {response_id} after {ARCHIVE_ATTEMPTS} attempts: "
        f"{last_error}. The query is unaffected; it just won't be readable "
        "once its Redis copy expires."
    )
    return False


async def archive_query(
    query_id: str,
    response_id: str,
    response_blob: Optional[bytes],
    status: Optional[str],
    description: Optional[str],
    logger: logging.Logger,
) -> bool:
    """Write a finished query's archive: the response ``capture_response``
    read, its query, and how it ended with its logs.

    Called after the query is delivered and the last logs are flushed. True
    when the object was written. Never raises, and never takes longer than
    ``ARCHIVE_TIMEOUT_SEC``.
    """
    try:
        if (
            not response_blob
            or not payload_store.enabled()
            or not _is_archivable(response_id)
        ):
            return False
        if bytes(response_blob[:4]) != ZSTD_MAGIC:
            # Every stored response is a zstd frame (encode_message); one
            # that isn't could not be told apart from the frames after it.
            logger.warning(f"Not archiving {response_id}: it is not a zstd frame")
            return False
        async with asyncio.timeout(ARCHIVE_TIMEOUT_SEC):
            blob = await _build(
                response_blob, query_id, response_id, status, description, logger
            )
            return await _write_with_retries(response_id, blob, logger)
    except TimeoutError:
        logger.error(
            f"Gave up archiving {response_id} after {ARCHIVE_TIMEOUT_SEC:g}s: "
            "the store is not answering. The query is unaffected; it just "
            "won't be readable once its Redis copy expires."
        )
        return False
    except Exception as e:
        logger.error(f"Could not archive {response_id}: {e}")
        return False


def split_frames(blob: bytes) -> List[bytes]:
    """Each zstd frame of ``blob``, decompressed, in order."""
    documents = []
    data = blob
    while data:
        decompressor = zstandard.ZstdDecompressor().decompressobj()
        documents.append(decompressor.decompress(data))
        if not decompressor.eof:
            raise ValueError("truncated zstd frame")
        data = decompressor.unused_data
    return documents


def parse_archive(blob: bytes) -> Archive:
    """Take an archived object apart. Raises ValueError (or a zstd/JSON
    error) for anything that is not one."""
    documents = split_frames(blob)
    if len(documents) != FRAMES:
        raise ValueError(f"expected {FRAMES} frames, found {len(documents)}")
    response, query_doc, tail_doc = documents
    query = orjson.loads(query_doc)
    tail = orjson.loads(tail_doc)
    if not isinstance(tail, dict):
        raise ValueError("the tail is not a JSON object")
    logs = tail.get("logs")
    logs = [entry for entry in logs if isinstance(entry, dict)] if logs else []
    # the same ordering get_logs gives the live list
    logs.sort(key=lambda entry: entry.get("timestamp", ""))
    return Archive(
        response=response,
        query=query if isinstance(query, dict) else None,
        status=tail.get("status"),
        description=tail.get("description"),
        logs=logs,
    )


def load_archive_sync(response_id: Any) -> Optional[Archive]:
    """A query's archive, or None when there is no (readable) one. Blocking:
    for the process-pool workers, and ``load_archive`` runs it in a thread."""
    if not payload_store.enabled() or not _is_archivable(response_id):
        return None
    blob = payload_store.read(response_id, payload_store.ARA)
    if blob is None:
        return None
    return parse_archive(blob)


async def load_archive(response_id: Any, logger: logging.Logger) -> Optional[Archive]:
    """A query's archive, or None when there is none. Redis is not
    re-warmed: a response read back from the archive is being looked at,
    not worked on."""
    try:
        return await asyncio.to_thread(load_archive_sync, response_id)
    except Exception as e:
        logger.error(f"Could not read the archive of {response_id}: {e}")
        return None
