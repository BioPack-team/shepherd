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

from shepherd_utils.ars.handoff import parse_handoff_callback
from shepherd_utils.broker import add_task, mark_task_as_complete
from shepherd_utils.db import (
    ResponseTooLargeError,
    cleanup_callbacks,
    enforce_response_size_limit,
    get_logs,
    get_message,
    get_query_state,
    save_logs,
    set_query_completed,
)
from shepherd_utils.response_limit import (
    TOO_LARGE_STATUS,
    fail_response_too_large,
    get_too_large_reason,
    write_too_large_response,
)
from shepherd_utils.shared import get_tasks
from shepherd_utils.trapi import (
    BIOLINK_VERSION,
    SCHEMA_VERSION,
    query_parameters,
)
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


def delivery_payload(stored: bytes, query: "dict | None", logs: "list[dict]") -> bytes:
    """The TRAPI 2.0 Response delivered for ``stored``, without decoding it.

    A stored response never carries the delivery envelope (see
    ``shepherd_utils.trapi.prepare_stored_response``) and its content is
    already valid 2.0, so the envelope -- ``schema_version``,
    ``biolink_version``, the query's ``parameters`` (which 2.0 says the server
    MUST repeat) -- is written in front of the stored members, and the logs
    after them. ``logs`` are last so ``_append_log_entry`` can extend them,
    and absent when there are none (``Response.logs`` has a ``minItems`` of
    1). One allocation of the payload's size; the slices are views. Anything
    that is not a JSON object is returned untouched.
    """
    envelope: dict = {
        "schema_version": SCHEMA_VERSION,
        "biolink_version": BIOLINK_VERSION,
    }
    parameters = query_parameters(query)
    if parameters:
        envelope["parameters"] = parameters
    head = orjson.dumps(envelope)
    view = memoryview(stored)
    # ``stored`` is one JSON object as orjson wrote it: no whitespace, so its
    # members are everything between the outer braces.
    if stored[:1] != b"{" or stored[-1:] != b"}":
        # Not something a worker stored; deliver it untouched rather than
        # guess at its structure.
        return stored
    members = view[1:-1]
    parts = [memoryview(head)[:-1]]
    if len(members):
        parts += [b",", members]
    if logs:
        parts += [b',"logs":', orjson.dumps(logs)]
    parts.append(b"}")
    return b"".join(parts)


def _append_log_entry(payload: bytes, entry: dict, has_logs: bool = True) -> bytes:
    """Return ``payload`` with ``entry`` appended to its trailing logs array.

    Only sound for a payload this worker built (``delivery_payload``): with
    ``has_logs`` it ends with the logs array and the closing brace; without,
    it has no ``logs`` member at all (an empty one is invalid TRAPI 2.0), so
    one is added. Rebuilding costs a transient second copy of the payload, so
    callers guard on size; the rebind releases the old buffer immediately. If
    the payload doesn't have the expected tail, hand it back untouched rather
    than risk shipping malformed JSON.
    """
    entry_bytes = orjson.dumps(entry)
    if not has_logs:
        if payload.endswith(b"}"):
            return payload[:-1] + b',"logs":[' + entry_bytes + b"]}"
        return payload
    if payload.endswith(b"[]}"):
        return payload[:-3] + b"[" + entry_bytes + b"]}"
    if payload.endswith(b"]}"):
        return payload[:-2] + b"," + entry_bytes + b"]}"
    return payload


async def send_callback(
    callback_url: str,
    message_bytes: bytes,
    logger: logging.Logger,
    has_logs: bool = True,
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
                        message_bytes, _log_entry(failure), has_logs
                    )
                    has_logs = True
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


async def _too_large_reason(
    query_id: str, response_id: str, logger: logging.Logger
) -> "str | None":
    """Why this response was (or is now) discarded as too large, else None.

    A response an earlier stage already discarded carries a marker. One that
    nobody checked -- the merge is the usual grower, but it only checks when
    the cap is on -- is checked here from the zstd header before the raw load
    below, so this worker can't be OOM-killed holding it either.
    """
    reason = await get_too_large_reason(response_id)
    if reason is not None:
        return reason
    try:
        await enforce_response_size_limit(response_id, logger)
    except ResponseTooLargeError as e:
        reason = f"finish_query: {e}"
        await fail_response_too_large(query_id, response_id, reason, logger)
        return reason
    return None


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
        too_large = await _too_large_reason(query_id, response_id, logger)
        if too_large is not None:
            # The response was discarded as too large somewhere along the way.
            # Whatever the task says, that is the status the query ends with,
            # and what gets delivered is the empty message saying so. It is
            # rewritten here rather than trusted: a merge pass that was
            # already in flight when the query was failed can have saved the
            # big accumulator over the top of it.
            status = TOO_LARGE_STATUS
            logger.error(
                f"Query {query_id} finishing with status {status}: {too_large}"
            )
            too_large_response = await write_too_large_response(
                query_id, response_id, too_large, logger
            )
        callback_url = query_state[8]
        ars_child_pk = parse_handoff_callback(callback_url)
        if ars_child_pk is not None:
            # The caller is this deployment's ARS: hand the response over on
            # the queue (the ARS has no HTTP callback endpoint). The
            # intake in ars_premerge loads the payload and logs from the blob
            # store by response_id itself, so nothing large is even resident
            # here. If the enqueue fails after its retries, the child stays
            # Running for the ARS watchdog -- the same terminal shape as an
            # undeliverable HTTP callback.
            carrier: dict = {}
            inject(carrier)
            try:
                await add_task(
                    "ars.premerge",
                    {
                        "intake_child_pk": ars_child_pk,
                        "response_id": response_id,
                        "query_id": query_id,
                        "otel": orjson.dumps(carrier).decode(),
                    },
                    logger,
                    raise_on_failure=True,
                )
                logger.info(
                    f"Handed response {response_id} to the ARS intake for "
                    f"child {ars_child_pk}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to enqueue ARS intake for child {ars_child_pk}: "
                    f"{e}. The response was not delivered."
                )
        elif callback_url is not None:
            # this was an async query, need to send message back
            if too_large is not None:
                message_bytes = orjson.dumps(too_large_response)
            else:
                message_bytes = await get_message(response_id, logger, raw=True)
            logs = await get_logs(response_id, logger)
            try:
                original_query = await get_message(query_id, logger)
            except Exception as e:
                # The query blob can have expired under a long-running query;
                # that must not cost the caller their response, only the
                # parameters echo.
                logger.warning(
                    f"Couldn't load query {query_id} to echo its parameters: {e}"
                )
                original_query = None
            # Build the TRAPI 2.0 Response around the stored bytes rather
            # than decoding them: the decoded tree is several times the size
            # of its JSON, and this worker holds many responses at once.
            # Rebinding releases the stored buffer as soon as the payload is
            # built, so only one full copy stays resident for the (up to
            # 120s x retries) POST below.
            message_bytes = delivery_payload(message_bytes, original_query, logs)
            has_logs = bool(logs)
            del logs, original_query

            await send_callback(callback_url, message_bytes, logger, has_logs)
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
