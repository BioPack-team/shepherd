"""Mark a query as completed and do any callbacks."""

import asyncio
import httpx
import logging
import time
import uuid
import orjson

from datetime import datetime, timezone

from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode, get_current_span

from shepherd_utils.broker import mark_task_as_complete
from shepherd_utils.db import (
    cleanup_callbacks,
    get_logs,
    get_message,
    get_query_state,
    save_logs,
    set_query_completed,
)
from shepherd_utils.shared import get_tasks
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "finish_query"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)
# Retries *after* the first attempt, so the total number of POSTs is
# CALLBACK_ATTEMPTS. Kept deliberately small: this worker holds the entire
# (potentially very large) decompressed response in memory for every second a
# callback is in flight, and each attempt can burn CALLBACK_TIMEOUT seconds
# before it even fails. A long retry budget therefore multiplies the worker's
# peak memory residency far more than it improves delivery odds.
CALLBACK_RETRIES = 1
CALLBACK_ATTEMPTS = CALLBACK_RETRIES + 1
CALLBACK_TIMEOUT = 120
# 4xx codes that describe a *transient* condition and explicitly invite another
# attempt, unlike the rest of the 4xx range. See ``_is_retryable``.
RETRYABLE_CLIENT_ERROR_STATUS = frozenset({408, 429})
# How much of a rejecting server's response body goes into the failure log. The
# body is already in memory (we don't stream the response), but it can be an
# arbitrarily large HTML error page, and this string is copied into the query's
# logs -- and possibly into the retry payload -- so keep only the head of it.
CALLBACK_ERROR_BODY_BYTES = 500
# Ceiling on the payload size for which we splice a failed attempt's note into
# the *next* attempt's body. Splicing rebuilds the whole buffer, so for a large
# response the transient second copy costs far more than the note is worth. The
# note is in the query's own logs either way, so oversized payloads just skip
# the inline copy.
RETRY_LOG_SPLICE_MAX_BYTES = 64 * 1024 * 1024


def _log_entry(message: str, level: str = "ERROR") -> dict:
    """Build a TRAPI LogEntry, matching ReasonerLogEntryFormatter's shape.

    Used for entries we splice straight into an outgoing payload, which never
    pass through the logging handler that would otherwise format them.
    """
    return {
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
    }


def _describe_callback_failure(e: Exception) -> str:
    """One bounded line explaining why a callback POST failed.

    The reason is the whole point of logging the failure -- "callback failed"
    alone doesn't say whether the receiver is down, slow, or rejecting the
    payload -- so pull out the status code and the head of the response body
    for an HTTP error, and the exception type otherwise (httpx reports connect
    failures, TLS errors and timeouts as distinct classes, and several of them
    stringify to an empty message).
    """
    if isinstance(e, httpx.HTTPStatusError):
        detail = ""
        try:
            body = e.response.content[:CALLBACK_ERROR_BODY_BYTES]
            if body:
                detail = f": {body.decode('utf-8', 'replace')}"
        except Exception:
            # Body not readable (streamed/closed response) -- the status code
            # is still worth reporting on its own.
            pass
        return f"HTTP {e.response.status_code}{detail}"
    if isinstance(e, httpx.TimeoutException):
        return f"{type(e).__name__} (no response within {CALLBACK_TIMEOUT}s)"
    return f"{type(e).__name__}: {e}"


def _is_retryable(e: Exception) -> bool:
    """Whether another attempt at this callback could plausibly succeed.

    A 4xx means the receiver understood the request and rejected it, so sending
    the same bytes again gets the same answer. Retrying is not merely useless
    here: this worker keeps the whole payload resident for every second of
    every attempt, so a doomed retry costs real memory on a worker whose peak
    memory is what gets it OOM-killed. The exceptions are the 4xx codes that
    signal a transient condition rather than a bad request.

    Everything else -- 5xx, timeouts, connect/protocol failures, and any
    exception we don't recognize -- stays retryable, so this narrows the retry
    loop only where a retry is known to be pointless.
    """
    if isinstance(e, httpx.HTTPStatusError):
        status = e.response.status_code
        if 400 <= status < 500:
            return status in RETRYABLE_CLIENT_ERROR_STATUS
    return True


def _append_log_entry(payload: bytes, entry: dict) -> bytes:
    """Return ``payload`` with ``entry`` appended to its trailing logs array.

    Only sound for a payload this worker built, which always ends with the logs
    array followed by the closing brace. Rebuilding costs a transient second
    copy of the payload, so callers guard on size; the rebind releases the old
    buffer immediately. If the payload doesn't have the expected tail, hand it
    back untouched rather than risk shipping malformed JSON.
    """
    entry_bytes = orjson.dumps(entry)
    if payload.endswith(b"[]}"):
        return payload[:-3] + b"[" + entry_bytes + b"]}"
    if payload.endswith(b"]}"):
        return payload[:-2] + b"," + entry_bytes + b"]}"
    return payload


async def send_callback(
    callback_url: str,
    message_bytes: bytes,
    logger: logging.Logger,
) -> bool:
    """POST the finished response to the caller's callback URL.

    Every attempt is timed and logged *after* the send completes -- how long a
    callback takes is a property of the receiver we otherwise have no record
    of, and a failure is only actionable with the reason attached. Failures are
    also spliced into the next attempt's payload (size permitting), so a
    receiver that eventually gets the response can see the attempts that didn't
    make it.

    Retries stop early on a failure ``_is_retryable`` rules out -- a payload the
    receiver has rejected outright is not worth holding in memory for another
    round trip.

    Returns True if the response was delivered.
    """
    headers = {"Content-Type": "application/json"}
    # Propagate the otel trace context through the callback.
    # Matches the inject() carrier pattern used by the
    # lookup workers; the active span comes from process_task's
    # start_as_current_span.
    inject(headers)
    span = get_current_span()
    started = time.time()
    payload_size = len(message_bytes)
    delivered = False
    retryable = True
    attempts = 0
    wait = 0.0
    backoff = 0.0
    for attempt in range(1, CALLBACK_ATTEMPTS + 1):
        attempts = attempt
        attempt_start = time.time()
        try:
            async with httpx.AsyncClient(timeout=CALLBACK_TIMEOUT) as client:
                response = await client.post(
                    callback_url,
                    content=message_bytes,
                    headers=headers,
                )
                response.raise_for_status()
                elapsed = time.time() - attempt_start
                wait += elapsed
                logger.info(
                    f"Sent response back to {callback_url} in {elapsed:.3f}s "
                    f"({len(message_bytes)} bytes, "
                    f"attempt {attempt}/{CALLBACK_ATTEMPTS})"
                )
                delivered = True
                break
        except Exception as e:
            elapsed = time.time() - attempt_start
            wait += elapsed
            reason = _describe_callback_failure(e)
            failure = (
                f"Failed to send callback to {callback_url} after {elapsed:.3f}s "
                f"(attempt {attempt}/{CALLBACK_ATTEMPTS}, "
                f"{len(message_bytes)} bytes): {reason}"
            )
            logger.error(failure)
            span.add_event(
                "callback.attempt_failed",
                {
                    "callback.attempt": attempt,
                    "callback.attempt_duration_ms": int(elapsed * 1000),
                },
            )
            if not _is_retryable(e):
                retryable = False
                logger.error(
                    f"Not retrying the callback to {callback_url}: {reason} is a "
                    "client error, so an identical retry would be rejected the "
                    "same way."
                )
                break
            if attempt < CALLBACK_ATTEMPTS:
                if len(message_bytes) <= RETRY_LOG_SPLICE_MAX_BYTES:
                    message_bytes = _append_log_entry(
                        message_bytes, _log_entry(failure)
                    )
                sleep_for = 1 * (2 ** (attempt - 1))
                backoff += sleep_for
                await asyncio.sleep(sleep_for)

    total = time.time() - started
    if not delivered:
        logger.error(
            f"Gave up sending callback to {callback_url} after "
            f"{attempts} attempt(s) and {total:.3f}s. The response was "
            "not delivered."
        )
    elif attempts > 1:
        logger.info(
            f"Callback to {callback_url} succeeded on attempt {attempts} "
            f"after {total:.3f}s total."
        )
    # Attributes rather than another log line: same numbers, no per-query log
    # storage, and they're queryable alongside the rest of the trace.
    span.set_attribute("callback.duration_ms", int(total * 1000))
    span.set_attribute("callback.wait_ms", int(wait * 1000))
    span.set_attribute("callback.backoff_ms", int(backoff * 1000))
    span.set_attribute("callback.attempts", attempts)
    # False means we stopped before spending the budget because the receiver
    # rejected the payload outright -- distinguishes "gave up" from "ran out".
    span.set_attribute("callback.retryable", retryable)
    span.set_attribute("callback.payload_bytes", payload_size)
    span.set_attribute("callback.delivered", delivered)
    return delivered


async def finish_query(task, logger: logging.Logger):
    """Do all the wrap up necessary for a query."""
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    response_id = task[1]["response_id"]
    status = task[1].get("status", "OK")
    query_state = await get_query_state(query_id, logger)

    if query_state is None:
        logger.error(f"Query id {query_id} not found in db.")
    else:
        callback_url = query_state[8]
        if callback_url is not None:
            # this was an async query, need to send message back
            message_bytes = await get_message(response_id, logger, raw=True)
            logs = await get_logs(response_id, logger)
            logs_bytes = orjson.dumps(logs)
            # Splice logs into the raw JSON bytes to avoid deserializing and
            # re-serializing the (potentially huge) message dict. We rebind
            # message_bytes to the spliced result so the original buffer is
            # released as soon as the new one is built -- otherwise both full
            # copies would stay resident for the entire (up to 120s x retries)
            # POST below, doubling this worker's peak memory under load.
            if message_bytes and message_bytes[-1:] == b"}":
                last_brace = message_bytes.rindex(b"}")
                message_bytes = (
                    message_bytes[:last_brace] + b',"logs":' + logs_bytes + b"}"
                )
            else:
                message = orjson.loads(message_bytes)
                # Re-insert rather than assign in place so "logs" is last in
                # the serialized payload -- send_callback appends retry notes
                # by rewriting the payload's tail.
                message.pop("logs", None)
                message["logs"] = logs
                message_bytes = orjson.dumps(message)
                del message
            # The logs list and its serialization are a full second copy of
            # every log line the query produced; they're inside the payload
            # now, so drop them before the send rather than holding them for
            # its duration.
            del logs, logs_bytes

            await send_callback(callback_url, message_bytes, logger)
            # Release the payload before the remaining db round trips.
            del message_bytes

        await set_query_completed(query_id, status, logger)

    # Always reap any callback rows tied to this query. Lookup workers do this
    # on timeout, but successful queries previously left rows behind forever.
    try:
        await cleanup_callbacks(query_id, logger)
    except Exception as e:
        logger.error(f"Failed to clean up callbacks for {query_id}: {e}")

    logger.info(f"Finished task {task[0]} in {time.time() - start}")

    # This worker acks directly instead of going through wrap_up_task, so
    # nothing else flushes what it logged. Persist here so the callback
    # outcome -- how long delivery took, or why it failed -- survives in the
    # query's logs (GET /response/{query_id}) instead of only in the pod's
    # stdout. Draining also clears the handler's queue, which for this
    # process-wide logger would otherwise just accumulate.
    try:
        await save_logs(response_id, logger)
    except Exception as e:
        logger.error(f"Failed to save logs for {response_id}: {e}")


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    start = time.time()
    with tracer.start_as_current_span(STREAM, context=parent_ctx) as span:
        try:
            await finish_query(task, logger)
        except asyncio.CancelledError:
            logger.warning(f"Task {task[0]} was cancelled")
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(
                f"Task {task[0]} failed with unhandled error: {e}", exc_info=True
            )
        finally:
            # Always wrap up the task to ACK it in the broker
            try:
                await mark_task_as_complete(STREAM, GROUP, task[0], logger)
            except Exception as e:
                logger.error(f"Task {task[0]}: Failed to wrap up task: {e}")
            limiter.release()
            logger.debug(f"Finished task {task[0]} in {time.time() - start}")


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
