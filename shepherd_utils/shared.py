"""Shared Shepherd Utility Functions."""

import asyncio
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import AsyncGenerator, Dict, List, Tuple

from opentelemetry import trace
from opentelemetry.context.context import Context
from opentelemetry.propagate import extract
from opentelemetry.trace import Status, StatusCode

from .broker import (
    add_task,
    broker_client,
    broker_health,
    get_task,
    mark_task_as_complete,
)
from .config import settings
from .db import initialize_db, save_logs
from .heartbeat import Heartbeat
from .logger import attach_query_handler, resolve_log_level, setup_logging
from .reclaim import reclaim_orphaned
from .task_deadline import (
    TIMEOUT_STATUS,
    carry_deadline,
    seconds_overdue,
)

# Cap each per-stream duration queue so a stopped monitor can't OOM the broker.
# 10k entries per stream is well above what we'd accumulate in a 30s drain
# window even at peak load.
_DURATION_QUEUE_CAP = 10000


def _duration_key(stream: str) -> str:
    return f"monitor:task_durations:{stream}"


async def _record_task_duration(
    stream: str,
    started_at_str: str,
    logger: logging.Logger,
) -> None:
    """Push ``ms_elapsed`` onto the per-stream duration list for the monitor."""
    if not started_at_str:
        return
    try:
        duration_ms = max(0, int((time.time() - float(started_at_str)) * 1000))
    except (TypeError, ValueError):
        return
    try:
        pipe = broker_client.pipeline()
        pipe.lpush(_duration_key(stream), str(duration_ms))
        pipe.ltrim(_duration_key(stream), 0, _DURATION_QUEUE_CAP - 1)
        await pipe.execute()
    except Exception as e:
        logger.debug(f"Failed to record task duration for {stream}: {e}")


setup_logging()


# ---------------------------------------------------------------------------
# Graceful shutdown / in-flight drain
#
# Kubernetes sends SIGTERM on every rollout, scale-down and node drain. Without
# handling it the worker is killed mid-task and the work is only recovered later
# via Redis reclaim. Here we install asyncio-aware signal handlers that flip a
# shutdown flag; ``get_tasks`` then stops pulling new work and drains anything
# in flight before the process exits.
#
# Draining piggybacks on the concurrency semaphore that ``get_tasks`` already
# owns: every worker acquires a permit before a task starts and releases it when
# the task finishes (in run_task_lifecycle / each worker's process_task finally).
# So "all permits acquired" is equivalent to "no task in flight" -- we don't need
# the workers to register their background tasks with us.
# ---------------------------------------------------------------------------

_shutdown = asyncio.Event()
_active_heartbeat: "Heartbeat | None" = None
_signal_handlers_installed = False
_loop_watchdog: "LoopWatchdog | None" = None


def is_shutting_down() -> bool:
    return _shutdown.is_set()


def _request_shutdown() -> None:
    _shutdown.set()


def _hard_exit(code: int) -> None:
    """Terminate the process immediately, bypassing atexit handlers.

    ``os._exit`` skips the interpreter-shutdown atexit join of the process pool,
    which could otherwise block a self-heal restart if a pool child is wedged
    mid-task. Used for the involuntary exits (broker wedge, loop watchdog) where
    getting the pod recycled promptly matters more than clean teardown. Wrapped
    so tests can substitute it instead of actually killing the test process.
    """
    os._exit(code)


class LoopWatchdog:
    """Force-exits the process if the asyncio event loop stops ticking.

    An asyncio task bumps ``_last_tick`` every ``tick_interval`` seconds. A
    separate daemon *thread* -- deliberately off the loop, so it keeps running
    even when the loop is wedged -- checks how long it's been since the last
    tick. If the loop has been blocked longer than ``stall_timeout`` the process
    is hard-exited (``os._exit``, bypassing atexit so a stuck pool can't block
    the exit) and Kubernetes restarts it. This turns any loop wedge into a
    restart instead of an indefinite hang whose heartbeat has silently died.

    Skips firing while a shutdown is in progress -- the drain path owns that exit
    and a slow drain must not be mistaken for a wedge.
    """

    def __init__(
        self, stall_timeout_sec: float, tick_interval_sec: float = 1.0, on_stall=None
    ):
        self._stall_timeout = stall_timeout_sec
        self._tick_interval = tick_interval_sec
        self._on_stall = on_stall or self._force_exit
        self._last_tick = time.monotonic()
        self._tick_task: "asyncio.Task | None" = None
        self._thread: "threading.Thread | None" = None

    def _stalled_for(self) -> float:
        return time.monotonic() - self._last_tick

    def _should_fire(self) -> bool:
        if is_shutting_down():
            return False
        return self._stalled_for() >= self._stall_timeout

    def _force_exit(self, stalled: float) -> None:
        try:
            sys.stderr.write(
                f"[loop-watchdog] event loop stalled {stalled:.0f}s "
                f">= {self._stall_timeout:.0f}s threshold; force-exiting for a "
                "clean restart.\n"
            )
            sys.stderr.flush()
        except Exception:
            pass
        os._exit(1)

    async def _tick_loop(self) -> None:
        while True:
            self._last_tick = time.monotonic()
            await asyncio.sleep(self._tick_interval)

    def _watch(self) -> None:
        while True:
            time.sleep(self._tick_interval)
            if self._should_fire():
                self._on_stall(self._stalled_for())
                return

    def start(self) -> "LoopWatchdog":
        self._last_tick = time.monotonic()
        self._tick_task = asyncio.create_task(self._tick_loop())
        self._thread = threading.Thread(
            target=self._watch, name="loop-watchdog", daemon=True
        )
        self._thread.start()
        return self


def install_shutdown_handlers(heartbeat: "Heartbeat | None" = None) -> None:
    """Install asyncio-aware SIGTERM/SIGINT handlers (idempotent).

    ``loop.add_signal_handler`` is the safe way to react to a signal from inside
    a running event loop: the callback runs between awaits rather than in the
    interrupt context, so it can flip an ``asyncio.Event`` the drain loop awaits.
    """
    global _signal_handlers_installed, _active_heartbeat, _loop_watchdog
    # Always update the heartbeat reference so the marker is written for the
    # currently-active worker even if get_tasks is re-entered after an error.
    _active_heartbeat = heartbeat
    if _signal_handlers_installed:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except (NotImplementedError, RuntimeError, ValueError):
            # Platforms without loop signal support fall back to signal.signal.
            try:
                signal.signal(sig, lambda *_: _request_shutdown())
            except (ValueError, OSError):
                pass
    # Loop-liveness watchdog: force-exit (for a Kubernetes restart) if the event
    # loop ever wedges, instead of hanging forever with a silently-dead
    # heartbeat. Installed alongside the shutdown handlers so it covers every
    # worker; a stall_limit of 0 disables it.
    stall_limit = float(settings.worker_loop_stall_exit_sec)
    if stall_limit > 0 and _loop_watchdog is None:
        _loop_watchdog = LoopWatchdog(stall_limit).start()
    _signal_handlers_installed = True


async def _drain_and_exit(
    limiter: asyncio.Semaphore,
    task_limit: int,
    logger: logging.Logger,
) -> None:
    """Wait for in-flight tasks to finish, mark a clean shutdown, then exit.

    Acquiring all ``task_limit`` permits means every in-flight task has released
    its permit -- i.e. completed. Bounded by ``worker_drain_timeout_sec``;
    stragglers are left in the stream for Redis reclaim to retry.
    """
    logger.info("Shutdown signal received; draining in-flight tasks.")
    acquired = 0

    async def _acquire_all() -> None:
        nonlocal acquired
        for _ in range(task_limit):
            await limiter.acquire()
            acquired += 1

    try:
        await asyncio.wait_for(
            _acquire_all(), timeout=float(settings.worker_drain_timeout_sec)
        )
        logger.info("All in-flight tasks drained cleanly.")
    except asyncio.TimeoutError:
        logger.warning(
            f"Drain timed out with ~{task_limit - acquired} task(s) still "
            "running; leaving them in the stream for Redis reclaim."
        )

    hb = _active_heartbeat
    if hb is not None:
        try:
            await hb.mark_clean_shutdown()
        except Exception as e:
            logger.debug(f"Failed to write clean shutdown marker: {e}")
        try:
            await hb.stop()
        except Exception:
            pass
    logger.info("Exiting after graceful drain.")
    sys.exit(0)


def _exit_if_broker_wedged(stream: str, logger: logging.Logger) -> None:
    """Self-heal: exit so Kubernetes replaces a worker wedged off the broker.

    A single worker can lose its broker connection (half-open socket, stale
    conntrack entry, a broker endpoint that moved) while every peer stays
    healthy. Its own retry loop can't recover because each reconnect traverses
    the same broken path -- but a rescheduled pod gets a fresh network setup.
    ``get_task`` keeps ``broker_health`` current; once we've gone longer than the
    configured window without a single successful read we exit non-zero.

    The window (``broker_unhealthy_exit_sec``) is generous on purpose so a real
    broker outage recycles the fleet slowly instead of crash-looping: every pod
    runs the full window before exiting, and the health clock starts fresh on
    boot. 0 disables the self-exit.
    """
    limit = float(settings.broker_unhealthy_exit_sec)
    if limit <= 0:
        return
    stale = broker_health.seconds_since_success()
    if stale >= limit:
        logger.error(
            f"Broker unreachable for {stale:.0f}s (>= {limit:.0f}s threshold) "
            f"after {broker_health.consecutive_failures} consecutive failures; "
            f"exiting so this {stream} worker is rescheduled with a fresh "
            "broker connection."
        )
        # Hard exit: the broker is unreachable, so there's nothing to flush, and
        # os._exit avoids blocking on the pool's atexit join if a child is busy.
        _hard_exit(1)


def _resolve_task_limit(stream: str, default: int, logger: logging.Logger) -> int:
    """Allow ops to override a worker's concurrency via the TASK_LIMIT env var.

    Each worker runs as its own container/Deployment, so a single ``TASK_LIMIT``
    env per Deployment unambiguously tunes that worker without a code change or
    rebuild. Falls back to the value the worker passed in.
    """
    raw = os.getenv("TASK_LIMIT")
    if raw is None:
        return default
    try:
        value = int(raw)
        if value < 1:
            raise ValueError
    except ValueError:
        logger.warning(f"Ignoring invalid TASK_LIMIT={raw!r} for {stream}.")
        return default
    if value != default:
        logger.info(f"TASK_LIMIT for {stream} overridden to {value} via env.")
    return value


def get_next_operation(
    workflow: List[Dict[str, str]],
) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """
    Get the next workflow operation from the list.

    Args:
        workflow (List[Dict[str, str]]): TRAPI workflow operation list
    """
    next_op = workflow[0]
    return next_op, workflow


def _build_task_context(
    stream: str,
    consumer: str,
    ara_task,
    level_number: int,
) -> Tuple[Context, logging.Logger]:
    """Build the per-task logger and otel context for a fetched/reclaimed task."""
    task_logger = logging.getLogger(
        f"shepherd.{stream}.{consumer}.{ara_task[1]['query_id']}"
    )
    task_log_level = int(ara_task[1].get("log_level", level_number))
    task_logger.setLevel(task_log_level)
    # One handler per logger: this logger is shared by every task this worker
    # runs for the query, and stacking a handler per task made each flush
    # re-persist the previous tasks' records.
    attach_query_handler(task_logger)
    task_logger.debug(f"Doing task {ara_task}")
    ctx = extract(json.loads(ara_task[1].get("otel", "{}")))
    # Stamp the task payload with our delivery time so wrap_up_task /
    # handle_task_failure can compute the per-task latency without touching
    # every individual worker. Only set if not already present so a reclaimed
    # task keeps its original start time.
    if "_started_at" not in ara_task[1]:
        ara_task[1]["_started_at"] = str(time.time())
    return ctx, task_logger


async def _terminate_task(
    stream: str,
    group: str,
    ara_task: Tuple[str, dict],
    logger: logging.Logger,
    reason: str,
    status: str = "ERROR",
) -> None:
    """Terminally clear a message: ack+delete it and end its query.

    Making a message terminal: ack + delete it (``mark_task_as_complete`` does
    ``XACK`` then ``XDEL``) so it leaves the PEL and the stream, and -- when we
    can still identify the parent query -- route it to ``finish_query`` so the
    query ends cleanly instead of hanging, mirroring ``handle_task_failure``.
    Shared by the "unprocessable message", "poison-pill / over-delivered" and
    "past its deadline" paths; ``status`` is what ``finish_query`` records in
    ``shepherd_brain`` and is what tells those cases apart afterwards.
    """
    msg_id = ara_task[0]
    fields = ara_task[1] if len(ara_task) > 1 and isinstance(ara_task[1], dict) else {}
    logger.error(f"Terminating task {msg_id} on {stream}: {reason}")
    query_id = fields.get("query_id")
    response_id = fields.get("response_id")
    if query_id and response_id:
        try:
            await add_task(
                "finish_query",
                {
                    "query_id": query_id,
                    "response_id": response_id,
                    "workflow": "[]",
                    "log_level": fields.get("log_level", 20),
                    "otel": fields.get("otel", "{}"),
                    "status": status,
                    "metadata": fields.get("metadata", "{}"),
                    **carry_deadline(fields),
                },
                logger,
            )
        except Exception as e:
            logger.error(
                f"Failed to route terminated task {msg_id} to finish_query: {e}"
            )
    # Remove it from the PEL + stream so it stops re-tripping stuck_pending and
    # can never be re-stranded under a live consumer.
    await mark_task_as_complete(stream, group, msg_id, logger)


async def _discard_unprocessable_task(
    stream: str,
    group: str,
    ara_task: Tuple[str, dict],
    logger: logging.Logger,
    reason: str,
) -> None:
    """Terminally clear a message we can't even build a task context for.

    Such a message is a poison pill. ``get_task`` reads with the ``>`` cursor,
    which never re-delivers a PEL entry, and ``reclaim_orphaned`` deliberately
    skips messages owned by a live/self consumer (``owner == consumer`` and
    ``owner in alive``). So if we merely dropped it from the poll loop it would
    sit in *this* (live) consumer's PEL forever -- invisible to reclaim and to
    the janitor, which only touches dead consumers. That is exactly the
    "very old unacked tasks on a still-running worker" leak.
    """
    fields = ara_task[1] if len(ara_task) > 1 and isinstance(ara_task[1], dict) else {}
    await _terminate_task(
        stream,
        group,
        ara_task,
        logger,
        f"unprocessable: {reason}. Payload fields: {sorted(fields.keys())}",
    )


# Streams whose tasks are never expired, however old their query is.
#
# ``finish_query`` *is* the wrap-up: expiring it would leave unset exactly the
# state expiring is meant to settle, and a query would never end.
# ``merge_message`` sits off the workflow chain -- its tasks are enqueued by the
# /callback endpoint for work an upstream service has already done and paid for,
# and dropping one would strand that callback in the ready index rather than
# saving anything.
_DEADLINE_EXEMPT_STREAMS = frozenset({"finish_query", "merge_message"})


async def _expire_task(
    stream: str,
    group: str,
    ara_task: Tuple[str, dict],
    logger: logging.Logger,
    overdue: float,
) -> None:
    """Wrap a query up instead of running an operation nobody is waiting for.

    The caller stopped waiting when the query passed its budget (see
    ``shepherd_utils.task_deadline``), so running this operation would spend a
    worker slot on an answer that can't be delivered -- and every operation
    after it would do the same. Instead the query finishes the ordinary way:
    ``finish_query`` sets its terminal state in Postgres, reaps its callback
    rows, saves its logs and POSTs whatever was gathered to the callback URL, so
    the databases end up exactly as they do for any completed query, with a
    ``TIMEOUT`` status recording why the response is partial.

    The explanation is logged and flushed to the query's own log list *before*
    the wrap-up is enqueued: ``finish_query`` reads those logs into the response
    it delivers, and it may well pick the task up before this coroutine returns.
    """
    fields = ara_task[1] if len(ara_task) > 1 and isinstance(ara_task[1], dict) else {}
    response_id = fields.get("response_id")
    reason = (
        f"query exceeded its time budget {overdue:.1f}s ago; skipping the "
        f"{stream} operation and returning what has been gathered so far"
    )

    async def _flush_logs() -> None:
        if not response_id:
            return
        try:
            await save_logs(response_id, logger)
        except Exception as e:
            logger.error(f"Failed to save logs for timed-out query: {e}")

    logger.warning(f"Query timed out: {reason}.")
    await _flush_logs()
    await _terminate_task(stream, group, ara_task, logger, reason, TIMEOUT_STATUS)
    # ``logger`` is this query's own logger, and what _terminate_task logged is
    # still sitting in its handler. Nothing else will run for this query, so
    # flush again rather than leaving the entry queued for a flush that never
    # comes.
    await _flush_logs()


async def _handled_as_expired(
    stream: str,
    group: str,
    ara_task: Tuple[str, dict],
    logger: logging.Logger,
) -> bool:
    """Whether this task's query is past its deadline (and has been wrapped up).

    Tasks with no deadline -- an exempt stream, a payload from a server that
    predates the field, or a deployment with the budget disabled -- are never
    expired, so this is a no-op unless a deadline says otherwise.
    """
    if stream in _DEADLINE_EXEMPT_STREAMS:
        return False
    overdue = seconds_overdue(ara_task[1] if len(ara_task) > 1 else None)
    if overdue <= 0:
        return False
    await _expire_task(stream, group, ara_task, logger, overdue)
    return True


class TaskSlots:
    """Concurrency limiter that also tracks how many tasks are *actively running*.

    ``get_tasks`` reserves a slot *before* it polls the broker for the next task
    -- the poll is a blocking read (a 5s ``XREADGROUP``). If the monitor counted
    every reserved slot as a running task, an idle worker that is only *waiting*
    for work would report a task in flight (the "shows one running even when it's
    just checking for a task" bug). So slot reservation (backpressure) is kept
    separate from dispatch (a task actually handed to a worker):

    * ``acquire`` / ``release_slot`` -- reserve/free a concurrency slot; used by
      ``get_tasks`` around the poll. These do **not** change the in-flight count.
    * ``dispatch`` -- called immediately before a task is yielded to a worker;
      marks one task as actively running.
    * ``release`` -- called by the worker's ``finally`` when the task finishes;
      frees the slot *and* clears the in-flight mark. Every worker already calls
      ``limiter.release()`` exactly once per task, so this is covered uniformly
      without touching each worker.

    ``in_flight`` is what the heartbeat surfaces as "Running". A plain
    :class:`asyncio.Semaphore` interface (``acquire``/``release``) is preserved
    so the drain path and workers keep working unchanged.
    """

    def __init__(self, task_limit: int):
        self._sem = asyncio.Semaphore(task_limit)
        self._in_flight = 0

    async def acquire(self) -> None:
        await self._sem.acquire()

    def release_slot(self) -> None:
        """Free a slot reserved for polling but never dispatched to a worker."""
        self._sem.release()

    def dispatch(self) -> None:
        """Mark a reserved slot as an actively-running task (about to be yielded)."""
        self._in_flight += 1

    def release(self) -> None:
        """A dispatched task finished: clear its in-flight mark and free its slot."""
        if self._in_flight > 0:
            self._in_flight -= 1
        self._sem.release()

    @property
    def in_flight(self) -> int:
        return self._in_flight


async def get_tasks(
    stream: str,
    group: str,
    consumer: str,
    task_limit: int,
    reclaim_min_idle_sec: int = None,
) -> AsyncGenerator[Tuple[Tuple[str, str], Context, logging.Logger, "TaskSlots"], None]:
    """Continually monitor the ara queue for tasks.

    ``reclaim_min_idle_sec`` overrides the per-stream default for how long a
    message must be idle before another consumer can XCLAIM it. Pass an
    explicit value when the worker knows its worst-case task duration; leave
    it ``None`` to fall back to ``PER_STREAM_MIN_IDLE_SEC`` / settings.
    """
    # Set up logger
    level_number = resolve_log_level(settings.log_level)
    worker_logger = logging.getLogger(f"shepherd.{stream}.{consumer}")
    worker_logger.setLevel(level_number)
    attach_query_handler(worker_logger)
    # allow ops to tune concurrency per Deployment without a code change
    task_limit = _resolve_task_limit(stream, task_limit, worker_logger)
    # initialize opens the db connection
    await initialize_db()
    # TaskSlots bounds concurrency like a Semaphore but distinguishes a slot
    # reserved for the blocking poll from a task actually dispatched to a worker,
    # so the heartbeat's "Running" count reflects real work, not idle polling.
    task_limiter = TaskSlots(task_limit)
    # register this worker with the monitor via a Redis heartbeat key. The
    # heartbeat does not install its own (immediate-exit) signal handlers --
    # install_shutdown_handlers below installs asyncio-aware ones that drain.
    heartbeat = Heartbeat(
        stream, consumer, task_limit, manage_signals=False, limiter=task_limiter
    ).start()
    install_shutdown_handlers(heartbeat)
    # periodic orphan-task reclaim so a worker crash doesn't strand its PEL
    reclaim_interval = max(5.0, float(settings.reclaim_interval_sec))
    last_reclaim = 0.0
    # after this many un-acked (re)deliveries a reclaimed message is treated as a
    # poison pill and dead-lettered rather than retried; 0 disables the breaker.
    max_task_deliveries = int(settings.max_task_deliveries)
    # continuously poll the broker for new tasks
    while True:
        # On shutdown, stop taking new work and drain anything in flight.
        if is_shutting_down():
            await _drain_and_exit(task_limiter, task_limit, worker_logger)
            return
        # Self-heal: if this worker has been unable to reach the broker for the
        # whole configured window, exit so Kubernetes reschedules it with a
        # fresh connection (its peers stay up; a wedged connection won't recover
        # on its own).
        _exit_if_broker_wedged(stream, worker_logger)
        # Before fetching new work, check whether any pending messages on this
        # stream belong to a dead consumer and claim them. Heartbeat + idle
        # filtering inside ``reclaim_orphaned`` keep live consumers safe.
        now = time.time()
        if now - last_reclaim >= reclaim_interval:
            last_reclaim = now
            delivery_counts: Dict[str, int] = {}
            try:
                reclaimed = await reclaim_orphaned(
                    stream,
                    group,
                    consumer,
                    worker_logger,
                    min_idle_sec=reclaim_min_idle_sec,
                    delivery_counts=delivery_counts,
                )
            except Exception as e:
                worker_logger.error(f"Reclaim sweep failed for {stream}: {e}")
                reclaimed = []
            for ara_task in reclaimed:
                # Poison-pill circuit breaker: a message that has been reclaimed
                # this many times without ever completing is almost certainly
                # crashing whatever worker takes it (e.g. an OOM SIGKILL no
                # in-process handler can catch). Dead-letter it instead of
                # spending another slot -- and another crash -- on it. Checked
                # before acquiring a permit so the terminal path needs no release.
                delivered = delivery_counts.get(ara_task[0], 0)
                if 0 < max_task_deliveries <= delivered:
                    await _terminate_task(
                        stream,
                        group,
                        ara_task,
                        worker_logger,
                        f"reclaimed {delivered} times without completing "
                        f"(>= max_task_deliveries={max_task_deliveries}); "
                        "treating as a poison pill",
                    )
                    continue
                await task_limiter.acquire()
                try:
                    ctx, task_logger = _build_task_context(
                        stream, consumer, ara_task, level_number
                    )
                except Exception as e:
                    # Poison pill: reclaim_orphaned has already XCLAIM'd this
                    # message into our (live) consumer's PEL, where nothing
                    # would ever reclaim it again. Make it terminal instead of
                    # leaking it back into the PEL.
                    await _discard_unprocessable_task(
                        stream,
                        group,
                        ara_task,
                        worker_logger,
                        f"could not build context for reclaimed task: {e}",
                    )
                    task_limiter.release_slot()
                    continue
                # Reclaim can hand back a message that has been sitting in a
                # dead consumer's PEL for a while, so this is exactly where a
                # query is most likely to have outlived its budget.
                if await _handled_as_expired(stream, group, ara_task, task_logger):
                    task_limiter.release_slot()
                    continue
                # Dispatching real work: count it as in-flight until the worker
                # releases the slot in its finally.
                task_limiter.dispatch()
                yield ara_task, ctx, task_logger, task_limiter

        # check if we can take another task
        await task_limiter.acquire()
        # A shutdown may have arrived while we waited for a free slot; don't
        # fetch new work in that case -- release and drain.
        if is_shutting_down():
            task_limiter.release_slot()
            await _drain_and_exit(task_limiter, task_limit, worker_logger)
            return
        # get a new task for the given target
        ara_task = await get_task(stream, group, consumer, worker_logger)
        if ara_task is not None:
            try:
                ctx, task_logger = _build_task_context(
                    stream, consumer, ara_task, level_number
                )
            except Exception as e:
                # A message we can't build a context for was just delivered into
                # this consumer's PEL via '>'. It will never be re-read ('>'
                # only returns new messages) nor reclaimed (owned by us, a live
                # consumer), so terminate it here instead of leaking it.
                await _discard_unprocessable_task(
                    stream,
                    group,
                    ara_task,
                    worker_logger,
                    f"could not build context for delivered task: {e}",
                )
                task_limiter.release_slot()
                continue
            # The query may have run out of budget while this task waited its
            # turn in the stream (or while an earlier operation ran long). Wrap
            # it up rather than starting work whose answer arrives too late.
            if await _handled_as_expired(stream, group, ara_task, task_logger):
                task_limiter.release_slot()
                continue
            # send the task to a async background task
            # this could be async, multi-threaded, etc.
            task_limiter.dispatch()
            yield ara_task, ctx, task_logger, task_limiter
        else:
            # Poll returned nothing (idle): free the reserved slot. Nothing was
            # dispatched, so the in-flight count is untouched.
            task_limiter.release_slot()


async def wrap_up_task(
    stream: str,
    group: str,
    task: tuple[str, dict],
    logger: logging.Logger,
):
    """Call the next task and mark this one as complete."""
    workflow = json.loads(task[1]["workflow"])
    # remove the operation we just did
    if stream == workflow[0]["id"]:
        # make sure the worker is in the workflow
        # for entry workers, they won't match and we'll do the first operation
        workflow.pop(0)
    # grab the next operation in the list
    if len(workflow) > 0:
        next_op = workflow[0]["id"]
    else:
        next_op = "finish_query"
    logger.debug(f"Sending task to {next_op}")
    await add_task(
        next_op,
        {
            "query_id": task[1]["query_id"],
            "response_id": task[1]["response_id"],
            "workflow": json.dumps(workflow),
            "log_level": task[1].get("log_level", 20),
            "otel": task[1]["otel"],
            "metadata": task[1]["metadata"],
            # The budget is measured from intake, so it travels with the query
            # rather than restarting at each operation.
            **carry_deadline(task[1]),
        },
        logger,
    )

    await mark_task_as_complete(stream, group, task[0], logger)
    await save_logs(task[1]["response_id"], logger)
    await _record_task_duration(stream, task[1].get("_started_at", ""), logger)


async def handle_task_failure(
    stream: str,
    group: str,
    task: Tuple[str, dict],
    logger: logging.Logger,
) -> None:
    """Handle any full query failures."""
    await mark_task_as_complete(stream, group, task[0], logger)
    await save_logs(task[1]["response_id"], logger)
    await _record_task_duration(stream, task[1].get("_started_at", ""), logger)
    logger.error("Sending task straight to finish_query.")
    await add_task(
        "finish_query",
        {
            "query_id": task[1]["query_id"],
            "response_id": task[1]["response_id"],
            "workflow": "[]",
            "log_level": task[1].get("log_level", 20),
            "otel": task[1]["otel"],
            "status": "ERROR",
            "metadata": task[1]["metadata"],
            **carry_deadline(task[1]),
        },
        logger,
    )


# Proxy tracer: resolves to whatever provider the worker process set up via
# setup_tracer(STREAM) at the time a span is created, so the outer task span
# inherits the worker's service.name without shared.py needing to know it.
_tracer = trace.get_tracer(__name__)


async def run_task_lifecycle(
    stream: str,
    group: str,
    task: Tuple[str, dict],
    parent_ctx: Context,
    logger: logging.Logger,
    limiter: asyncio.Semaphore,
    worker_fn,
) -> None:
    """Span-wrapped task lifecycle shared by the standard workers.

    Activates the per-task span as current so auto-instrumented (httpx) and
    manual child spans nest under it, records exceptions + ERROR status on the
    span, runs ``worker_fn(task, logger)`` then ``wrap_up_task`` on success or
    ``handle_task_failure`` on an unhandled error, and always releases the
    limiter.

    ``worker_fn`` is an async callable ``(task, logger) -> None`` holding the
    per-worker logic (or a closure for workers that dispatch to a process pool).
    """
    start = time.time()
    with _tracer.start_as_current_span(stream, context=parent_ctx) as span:
        try:
            await worker_fn(task, logger)
            # Always wrap up the task to ACK it in the broker
            try:
                await wrap_up_task(stream, group, task, logger)
            except Exception as e:
                logger.error(f"Task {task[0]}: Failed to wrap up task: {e}")
        except asyncio.CancelledError:
            logger.warning(f"Task {task[0]} was cancelled")
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(
                f"Task {task[0]} failed with unhandled error: {e}", exc_info=True
            )
            await handle_task_failure(stream, group, task, logger)
        finally:
            limiter.release()
            logger.debug(f"Finished task {task[0]} in {time.time() - start}")


def recursive_get_edge_support_graphs(
    edge: str,
    edges: set,
    auxgraphs: set,
    message_edges: dict,
    message_auxgraphs: dict,
    nodes: set,
):
    """Recursive method to find auxiliary graphs to keep when filtering. Each auxiliary
    graph then has its edges filterd."""
    if edge in edges:
        # Already visited; short-circuit to avoid exponential re-traversal
        # when many edges/aux graphs share the same support structure.
        return edges, auxgraphs, nodes
    edges.add(edge)
    edge_data = message_edges[edge]
    nodes.add(edge_data["subject"])
    nodes.add(edge_data["object"])
    for attribute in edge_data.get("attributes", []) or []:
        if attribute.get("attribute_type_id") == "biolink:support_graphs":
            for auxgraph in attribute.get("value", []):
                if auxgraph not in message_auxgraphs:
                    raise KeyError(f"auxgraph {auxgraph} not in auxiliary_graphs")
                edges, auxgraphs, nodes = recursive_get_auxgraph_edges(
                    auxgraph,
                    edges,
                    auxgraphs,
                    message_edges,
                    message_auxgraphs,
                    nodes,
                )
    return edges, auxgraphs, nodes


def recursive_get_auxgraph_edges(
    auxgraph: str,
    edges: set,
    auxgraphs: set,
    message_edges: dict,
    message_auxgraphs: dict,
    nodes: set,
):
    """Recursive method to find edges to keep when filtering. Each edge then
    has support graphs filtered."""
    if auxgraph in auxgraphs:
        return edges, auxgraphs, nodes
    auxgraphs.add(auxgraph)
    aux_edges = message_auxgraphs.get(auxgraph, {}).get("edges", [])
    for aux_edge in aux_edges:
        if aux_edge not in message_edges:
            raise KeyError(f"aux_edge {aux_edge} not in knowledge_graph.edges")
        edges, auxgraphs, nodes = recursive_get_edge_support_graphs(
            aux_edge, edges, auxgraphs, message_edges, message_auxgraphs, nodes
        )
    return edges, auxgraphs, nodes


def is_support_edge(edge) -> bool:
    """Checks if a given edge is a support edge."""
    if "attributes" not in edge:
        return False
    for attribute in edge["attributes"]:
        if attribute["attribute_type_id"] == "biolink:support_graphs":
            return True
    return False


def validate_message(message, logger):
    """Validate a given message for missing nodes."""
    valid = True
    for edge_id, edge in message["message"]["knowledge_graph"]["edges"].items():
        try:
            # print(f"Checking {edge_id}")
            assert edge["subject"] in message["message"]["knowledge_graph"]["nodes"]
            assert edge["object"] in message["message"]["knowledge_graph"]["nodes"]
            for attribute in edge.get("attibutes", []):
                if attribute["attribute_type_id"] == "biolink:support_graphs":
                    for value in attribute["value"]:
                        if value not in message["message"].get("auxiliary_graphs", {}):
                            raise AssertionError(
                                f"Aux graph {value} is not in the aux graphs."
                            )
        except AssertionError as e:
            valid = False
            logger.error(f"Edge {edge_id} has issues: {e}")
    if not valid:
        with open("invalid_message.json", "w", encoding="utf-8") as f:
            json.dump(message, f, indent=2)


def combine_unique_dicts(list1, list2, logger: logging.Logger):
    """Combine two lists of dicts, keeping only unique dictionaries.

    Uses ``json.dumps(..., sort_keys=True)`` as a stable signature -- it's
    implemented in C and faster than the recursive Python hashing the prior
    implementation used. ``default=str`` keeps it forgiving for the rare
    non-JSON-serializable value (datetime, Decimal, etc.) instead of dropping
    the item silently.
    """
    seen = set()
    result = []
    for d in list1:
        try:
            sig = json.dumps(d, sort_keys=True, default=str)
        except (TypeError, ValueError):
            logger.error(f"Failed to hash this: {d}")
            continue
        if sig not in seen:
            seen.add(sig)
            result.append(d)
    for d in list2:
        try:
            sig = json.dumps(d, sort_keys=True, default=str)
        except (TypeError, ValueError):
            logger.error(f"Failed to hash this: {d}")
            continue
        if sig not in seen:
            seen.add(sig)
            result.append(d)
    return result


def merge_kgraph(og_message, new_message, source, logger: logging.Logger):
    """Merge ``new_message`` into ``og_message`` in place and return it.

    Previously this allocated a deep copy of ``og_message`` and mutated that.
    The deep copy dominated runtime on large kgraphs (thousands of edges,
    each with attribute lists). The accumulator-style call sites
    (``acc = merge_kgraph(acc, kg, ...)``) discard ``og_message`` after
    each call, so mutating it directly is safe and dramatically faster.
    Newly adopted nodes/edges are not copied either -- ``new_message``
    is also discarded by the caller after merging.
    """
    aggregator_source = {
        "resource_id": source,
        "resource_role": "aggregator_knowledge_source",
        "upstream_resource_ids": ["infores:retriever"],
    }
    og_nodes = og_message["nodes"]
    og_edges = og_message["edges"]

    for key, value in new_message["nodes"].items():
        existing = og_nodes.get(key)
        if existing is None:
            og_nodes[key] = value
            continue
        # Overlapping node: merge fields onto the existing entry.
        if value["name"]:
            existing["name"] = value["name"]
        new_categories = value["categories"]
        if new_categories:
            existing_categories = existing["categories"]
            if existing_categories:
                existing["categories"] = list(
                    set(existing_categories) | set(new_categories)
                )
            else:
                existing["categories"] = new_categories
        new_attrs = value["attributes"]
        if new_attrs:
            existing_attrs = existing["attributes"]
            if existing_attrs:
                existing["attributes"] = combine_unique_dicts(
                    existing_attrs, new_attrs, logger
                )
            else:
                existing["attributes"] = new_attrs

    for key, value in new_message["edges"].items():
        existing = og_edges.get(key)
        if existing is None:
            og_edges[key] = value
            sources = value.get("sources")
            if sources and not is_support_edge(value):
                # Append the aggregator source if it isn't already present.
                # Avoids the heavy combine_unique_dicts hashing for what is
                # almost always a 3-element list.
                if aggregator_source not in sources:
                    sources.append(aggregator_source)
            continue
        # Overlapping edge: merge attributes and sources.
        new_attrs = value["attributes"]
        if new_attrs:
            existing_attrs = existing["attributes"]
            if existing_attrs:
                existing["attributes"] = combine_unique_dicts(
                    existing_attrs, new_attrs, logger
                )
            else:
                existing["attributes"] = new_attrs

        new_sources = value["sources"]
        if new_sources:
            existing_sources = existing["sources"]
            if existing_sources:
                # TODO: there might need to be some sort of upstream resource id merging to do past this?
                existing["sources"] = combine_unique_dicts(
                    existing_sources, new_sources, logger
                )
            else:
                existing["sources"] = new_sources

    return og_message


def filter_kgraph_orphans(message, logger: logging.Logger):
    """Given a result-pruned message, filter out orphaned kgraph nodes and edges."""
    try:
        results = message.get("message", {}).get("results", [])
        message_auxgraphs = message.get("message", {}).get("auxiliary_graphs", {})
        kg_edges = (
            message.get("message", {}).get("knowledge_graph", {}).get("edges", {})
        )
        nodes = set()
        edges = set()
        auxgraphs = set()
        temp_auxgraphs = set()
        temp_edges = set()
        # 1. Result node bindings
        for result in results:
            for _, knodes in result.get("node_bindings", {}).items():
                nodes.update([k["id"] for k in knodes])
        # 2. Result.Analysis edge bindings
        for result in results:
            for analysis in result.get("analyses", []):
                for _, kedges in analysis.get("edge_bindings", {}).items():
                    temp_edges.update([k["id"] for k in kedges])
                for _, path_graphs in analysis.get("path_bindings", {}).items():
                    temp_auxgraphs.update(a["id"] for a in path_graphs)
        # 3. Result.Analysis support graphs
        for result in results:
            for analysis in result.get("analyses", []):
                for auxgraph in analysis.get("support_graphs", []):
                    temp_auxgraphs.add(auxgraph)
        # 4. Support graphs from edges in 2
        for edge in temp_edges:
            try:
                edges, auxgraphs, nodes = recursive_get_edge_support_graphs(
                    edge,
                    edges,
                    auxgraphs,
                    kg_edges,
                    message_auxgraphs,
                    nodes,
                )
            except KeyError as e:
                logger.warning(f"Failed to get edge support graph {edge}: {e}")
                continue
        # 5. For all the auxgraphs collect their edges and nodes
        for auxgraph in temp_auxgraphs:
            try:
                edges, auxgraphs, nodes = recursive_get_auxgraph_edges(
                    auxgraph,
                    edges,
                    auxgraphs,
                    kg_edges,
                    message_auxgraphs,
                    nodes,
                )
            except KeyError as e:
                logger.warning(f"Failed to get auxgraph edges {auxgraph}: {e}")
                continue

        # make sure message and knowledge graph exist
        message["message"] = message.get("message") or {}
        message["message"]["knowledge_graph"] = message["message"].get(
            "knowledge_graph"
        ) or {
            "nodes": {},
            "edges": {},
        }
        # Now remove all knowledge_graph nodes/edges (and auxiliary graphs) that
        # aren't in our keep-sets. Delete in place rather than rebuilding each
        # dict with a comprehension: the knowledge graph is usually the largest
        # part of the response, and a comprehension would hold the full original
        # dict and the filtered copy at the same time -- doubling the dict
        # overhead for the biggest structure right before we re-encode it.
        # Deleting the orphans drops them (and everything they reference) now.
        kg_nodes = (
            message.get("message", {}).get("knowledge_graph", {}).get("nodes", {})
        )
        for nid in [nid for nid in kg_nodes if nid not in nodes]:
            del kg_nodes[nid]
        kg_edges = (
            message.get("message", {}).get("knowledge_graph", {}).get("edges", {})
        )
        for eid in [eid for eid in kg_edges if eid not in edges]:
            del kg_edges[eid]
        # validate_message(message)
        kg_auxgraphs = message["message"].get("auxiliary_graphs")
        if kg_auxgraphs:
            for auxgraph in [a for a in kg_auxgraphs if a not in auxgraphs]:
                del kg_auxgraphs[auxgraph]
        elif "auxiliary_graphs" not in message["message"]:
            # Preserve the prior behavior that this key is always present after
            # filtering (the old comprehension created an empty dict when the
            # response carried no auxiliary graphs).
            message["message"]["auxiliary_graphs"] = {}
        # is_invalid = validate_message(message)
        # if is_invalid:
        #     before_is_invalid = validate_message(initial_message)
        #     if not before_is_invalid:
        #         with open("before_filtering_message.json", "w") as f:
        #             json.dump(initial_message, f, indent=2)
        #         with open("invalid_after_message.json", "w") as f:
        #             json.dump(message, f, indent=2)
        #     else:
        #         print("this message was bad to begin with")
        #         with open("invalid_before_message.json", "w") as f:
        #             json.dump(initial_message, f, indent=2)
    except KeyError as e:
        # can't find the right structure of message
        logger.error(f"Error filtering kgraph orphans: {e}")
        # return message, 400


def examine_query(message):
    """Decides whether the input is an infer. Returns the grouping node"""
    # Currently, we support:
    # queries that are any shape with all lookup edges
    # OR
    # A 1-hop infer query.
    # OR
    # Pathfinder query
    try:
        # this can still fail if the input looks like e.g.:
        #  "query_graph": None
        qedges = message.get("message", {}).get("query_graph", {}).get("edges", {})
    except KeyError:
        qedges = {}
    try:
        # this can still fail if the input looks like e.g.:
        #  "query_graph": None
        qpaths = message.get("message", {}).get("query_graph", {}).get("paths", {})
    except KeyError:
        qpaths = {}
    if len(qpaths) > 1:
        raise Exception("Only a single path is supported")
    if (len(qpaths) > 0) and (len(qedges) > 0):
        raise Exception("Mixed mode pathfinder queries are not supported")
    pathfinder = len(qpaths) == 1
    n_infer_edges = 0
    for edge_id in qedges:
        if qedges.get(edge_id, {}).get("knowledge_type", "lookup") == "inferred":
            n_infer_edges += 1
    if n_infer_edges > 1 and n_infer_edges:
        raise Exception("Only a single infer edge is supported")
    if (n_infer_edges > 0) and (n_infer_edges < len(qedges)):
        raise Exception("Mixed infer and lookup queries not supported")
    infer = n_infer_edges == 1
    if not infer:
        return infer, None, None, pathfinder
    qnodes = message.get("message", {}).get("query_graph", {}).get("nodes", {})
    question_node = None
    answer_node = None
    for qnode_id, qnode in qnodes.items():
        if qnode.get("ids", None) is None:
            answer_node = qnode_id
        else:
            question_node = qnode_id
    if answer_node is None:
        raise Exception("Both nodes of creative edge pinned")
    if question_node is None:
        raise Exception("No nodes of creative edge pinned")
    return infer, question_node, answer_node, pathfinder
