"""The archive of finished ARA responses in the cold store.

Redis holds a query's response while the query runs. When the pipeline
finishes, ``finish_query`` copies the response here -- as it stands, before
anything downstream (a callback envelope, the ARS's premerge) touches it --
and, once it has finished logging, the query's logs beside it. That copy is
what stays readable for ``ara_response_retention_days`` after the Redis copy
has expired, for a query submitted directly and for one the ARS dispatched
alike.

The archive is a debugging record, never part of delivering a query: every
write here is best-effort. A failure is retried, then logged, and the query
is delivered regardless. Nothing is archived (and every read answers None)
unless a store is configured (``shepherd_utils/ars/payload_store.py``).

The archived response is the same zstd frame Redis held, in its stored form
(no delivery envelope; see ``shepherd_utils.trapi.prepare_stored_response``).
Logs are a zstd-compressed JSON list.
"""

import asyncio
import logging
from typing import Awaitable, Callable, List, Optional

import orjson
import zstandard

import shepherd_utils.db as shepherd_db
from shepherd_utils.ars import payload_store

ARCHIVE_ATTEMPTS = 3
# The most one archive write may hold up the query it belongs to, retries
# included. finish_query archives before it delivers, and an ARS child times
# out after minutes: a store that hangs must cost the copy, not the answer.
ARCHIVE_TIMEOUT_SEC = 30.0


def _is_archivable(message_id: str) -> bool:
    """Only UUID ids have a place in the store (``relative_key``); anything
    else -- an id from a caller's URL, say -- simply has no archive."""
    try:
        payload_store.relative_key(message_id)
    except (ValueError, AttributeError, TypeError):
        return False
    return True


async def _write_with_retries(
    message_id: str,
    kind: str,
    what: str,
    load: Callable[[], Awaitable[Optional[bytes]]],
    logger: logging.Logger,
) -> bool:
    """Write what ``load`` returns to the store, within
    ``ARCHIVE_TIMEOUT_SEC``. ``load`` runs inside the retry so a transient
    Redis error is retried with the write; it answers None when there is
    nothing to archive."""
    try:
        async with asyncio.timeout(ARCHIVE_TIMEOUT_SEC):
            return await _attempt_write(message_id, kind, what, load, logger)
    except TimeoutError:
        logger.error(
            f"Gave up archiving the {what} of {message_id} after "
            f"{ARCHIVE_TIMEOUT_SEC:g}s: the store is not answering. The query "
            "is unaffected; it just won't be readable once its Redis copy "
            "expires."
        )
        return False


async def _attempt_write(
    message_id: str,
    kind: str,
    what: str,
    load: Callable[[], Awaitable[Optional[bytes]]],
    logger: logging.Logger,
) -> bool:
    last_error: Optional[Exception] = None
    for attempt in range(ARCHIVE_ATTEMPTS):
        if attempt:
            await asyncio.sleep(0.1 * (2**attempt))
        try:
            blob = await load()
            if blob is None:
                return False
            await asyncio.to_thread(payload_store.write, message_id, blob, kind)
            return True
        except Exception as e:
            last_error = e
            logger.warning(
                f"Failed to archive the {what} of {message_id} "
                f"(attempt {attempt}): {e}"
            )
    logger.error(
        f"Could not archive the {what} of {message_id} after "
        f"{ARCHIVE_ATTEMPTS} attempts: {last_error}. The query is unaffected; "
        "it just won't be readable once its Redis copy expires."
    )
    return False


async def archive_response(response_id: str, logger: logging.Logger) -> bool:
    """Copy a finished response from Redis to the cold store.

    True when a copy was written. Never raises: the caller is finishing a
    query, and a missing debugging copy must not cost anyone their answer.
    """
    try:
        if not payload_store.enabled() or not _is_archivable(response_id):
            return False

        async def _load() -> Optional[bytes]:
            return await shepherd_db.data_db_client.get(response_id)

        return await _write_with_retries(
            response_id, payload_store.ARA, "response", _load, logger
        )
    except Exception as e:
        logger.error(f"Could not archive the response of {response_id}: {e}")
        return False


async def archive_logs(response_id: str, logger: logging.Logger) -> bool:
    """Copy a finished query's logs from Redis to the cold store.

    Called after the last flush of the query's logs, so the archived list is
    complete. True when a copy was written; never raises.
    """
    try:
        if not payload_store.enabled() or not _is_archivable(response_id):
            return False

        async def _load() -> Optional[bytes]:
            entries = await shepherd_db.logs_db_client.lrange(response_id, 0, -1)
            if not entries:
                return None
            # each entry is already one JSON document
            body = b"[" + b",".join(bytes(entry) for entry in entries) + b"]"
            return await asyncio.to_thread(zstandard.compress, body)

        return await _write_with_retries(
            response_id, payload_store.ARA_LOGS, "logs", _load, logger
        )
    except Exception as e:
        logger.error(f"Could not archive the logs of {response_id}: {e}")
        return False


async def _read(message_id: str, kind: str, logger: logging.Logger) -> Optional[bytes]:
    if not payload_store.enabled() or not _is_archivable(message_id):
        return None
    try:
        blob = await asyncio.to_thread(payload_store.read, message_id, kind)
    except Exception as e:
        logger.error(f"Archive read failed for {message_id}: {e}")
        return None
    if blob is None:
        return None
    try:
        return await asyncio.to_thread(shepherd_db.decompress_zstd, blob)
    except Exception as e:
        logger.error(f"Undecodable archived payload for {message_id}: {e}")
        return None


async def load_response_bytes(
    response_id: str, logger: logging.Logger
) -> Optional[bytes]:
    """An archived response as decompressed JSON bytes (never parsed), or
    None when there is no archived copy. Redis is not re-warmed: a response
    read back from the archive is being looked at, not worked on."""
    return await _read(response_id, payload_store.ARA, logger)


def load_response_sync(response_id: str) -> Optional[dict]:
    """``load_response_bytes`` for the process-pool workers, parsed: the
    archived response as a dict, or None when there is no (readable) copy."""
    if not payload_store.enabled() or not _is_archivable(response_id):
        return None
    try:
        blob = payload_store.read(response_id, payload_store.ARA)
        if blob is None:
            return None
        payload = shepherd_db.decode_message(blob)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


async def load_logs(response_id: str, logger: logging.Logger) -> List[dict]:
    """An archived query's logs, in chronological order; [] when there are
    none."""
    raw = await _read(response_id, payload_store.ARA_LOGS, logger)
    if raw is None:
        return []
    try:
        logs = orjson.loads(raw)
    except orjson.JSONDecodeError as e:
        logger.error(f"Undecodable archived logs for {response_id}: {e}")
        return []
    if not isinstance(logs, list):
        return []
    # the same ordering get_logs gives the live list
    logs.sort(key=lambda entry: entry.get("timestamp", ""))
    return logs
