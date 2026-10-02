"""Recover the queries a Redis restart left behind.

When the broker Redis restarts it comes back from its last snapshot (or AOF),
so everything written after that point is gone: the blobs of queries that were
in flight, their merge tasks, the callbacks they had received. Postgres keeps
going, so those queries' ``shepherd_brain`` rows still say they are running,
but nothing can finish them normally, and the abandoned-query reaper only
gets to them ~10 minutes later, without ever telling the caller.

The monitor already polls ``INFO`` every tick. Redis gives itself a new
``run_id`` every time it starts, so a changed ``run_id`` is a restart. On one
the monitor alerts, waits for Redis to finish loading its dataset and then for
a grace period, and sweeps the unfinished queries whose query or response blob
is gone into ``finish_query`` as errors -- which tells an HTTP caller with an
error response and fails an ARS child at once.

The grace period is what keeps a query from being finished twice: a worker
that was holding a lost query's task fails on the missing blob within seconds
and finishes the query itself, and a lookup still waiting on callbacks ends
its own query (``abandon_lookup_if_data_lost``) -- those are skipped by the
sweep (see ``find_queries_with_lost_data``). What is left after the grace
period is the queries nothing else is going to finish.
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

from shepherd_utils.broker import add_task, broker_client
from shepherd_utils.config import settings
from shepherd_utils.db import find_queries_with_lost_data

from . import alerts

logger = logging.getLogger("shepherd.monitor.recovery")

# The last Redis run_id the monitor saw, kept in Redis itself so a restart is
# still spotted when the monitor restarted too. A rolled-back copy of this key
# holds an older run_id, which reads as a restart -- as it should.
RUN_ID_KEY = "monitor:redis_run_id"
LOADING_POLL_SEC = 2.0
# Give up waiting for the load after this long and sweep anyway: a check made
# while Redis is still loading raises rather than reading as data loss.
LOADING_MAX_WAIT_SEC = 1800.0


async def _wait_until_loaded() -> None:
    """Block until Redis has finished loading its dataset after a restart."""
    deadline = time.time() + LOADING_MAX_WAIT_SEC
    while time.time() < deadline:
        try:
            info = await broker_client.info("persistence")
            if not int(info.get("loading", 0) or 0):
                return
        except Exception as e:
            logger.debug(f"Waiting for Redis to load: {e}")
        await asyncio.sleep(LOADING_POLL_SEC)
    logger.warning("Redis still loading after restart; sweeping lost queries anyway")


async def reconcile_lost_queries(min_age_sec: float) -> List[Dict[str, Any]]:
    """Finish, as errors, unfinished queries older than ``min_age_sec`` whose
    query or response blob is gone. Returns the queries it finished."""
    try:
        lost = await find_queries_with_lost_data(min_age_sec, logger)
    except Exception as e:
        logger.error(f"Couldn't check for queries that lost their data: {e}")
        return []
    finished: List[Dict[str, Any]] = []
    for query in lost:
        try:
            await add_task(
                "finish_query",
                {
                    "query_id": query["qid"],
                    "response_id": query["response_id"],
                    "workflow": "[]",
                    "log_level": logging.INFO,
                    "otel": "{}",
                    "status": "ERROR",
                    "metadata": "{}",
                },
                logger,
                raise_on_failure=True,
            )
        except Exception as e:
            logger.error(f"Couldn't hand lost query {query['qid']} to finish: {e}")
            continue
        finished.append(query)
    if finished:
        logger.warning(
            f"Finished {len(finished)} queries whose data was lost: "
            f"{[q['qid'] for q in finished]}"
        )
        await _alert_lost(finished)
    return finished


async def reconcile_after_restart(detected_at: float) -> List[Dict[str, Any]]:
    """The post-restart sweep: wait for the load and the grace period, then
    finish the queries from before the restart that lost their data."""
    await _wait_until_loaded()
    await asyncio.sleep(settings.monitor_redis_restart_grace_sec)
    return await reconcile_lost_queries(time.time() - detected_at)


class RedisRestartWatcher:
    """Spots a Redis restart from its ``run_id`` and starts the sweep."""

    def __init__(self) -> None:
        self._run_id: Optional[str] = None
        # The run_id last written to RUN_ID_KEY. Lags ``_run_id`` while Redis
        # refuses writes (it does while loading), so the write is retried.
        self._recorded: Optional[str] = None
        self._sweep: Optional[asyncio.Task] = None
        self._sweep_from = 0.0

    async def observe(self, snapshot: Dict[str, Any]) -> bool:
        """Check one poll snapshot; True when it shows a restart."""
        run_id = (snapshot.get("redis") or {}).get("run_id")
        if not run_id:
            return False
        previous = self._run_id
        if previous is None:
            # First tick since the monitor started: compare with the run_id
            # recorded before it did.
            try:
                previous = await broker_client.get(RUN_ID_KEY)
            except Exception as e:
                logger.debug(f"Couldn't read the last Redis run_id: {e}")
                return False
        self._run_id = run_id
        if self._recorded != run_id:
            try:
                await broker_client.set(RUN_ID_KEY, run_id)
                self._recorded = run_id
            except Exception as e:
                logger.debug(f"Couldn't record the Redis run_id: {e}")
        if previous == run_id:
            return False
        if previous is None:
            return False  # first time this Redis has been seen at all
        await self._on_restart(snapshot)
        return True

    async def _on_restart(self, snapshot: Dict[str, Any]) -> None:
        now = time.time()
        uptime = (snapshot.get("redis") or {}).get("uptime_in_seconds")
        logger.warning(
            f"Redis restarted (up {uptime}s); sweeping queries that lost their "
            f"data in {settings.monitor_redis_restart_grace_sec}s"
        )
        event = {
            "ts": now,
            "rule": "redis_restarted",
            "severity": "warning",
            "detail": f"broker Redis restarted (up {uptime}s)",
            "message": (
                "Broker `redis` restarted and came back from its last "
                "snapshot: anything written after it -- in-flight query data, "
                "queued tasks, received callbacks -- is gone. Queries that "
                "lost their data will be finished as errors once it has "
                "loaded."
            ),
        }
        await alerts._record_alert(event)
        await alerts.dispatch(event)
        if self._sweep is not None and not self._sweep.done():
            # Restarted again before the last sweep ran: sweep once, after
            # this restart has loaded, covering queries from before either.
            self._sweep.cancel()
        else:
            self._sweep_from = now
        self._sweep = asyncio.create_task(reconcile_after_restart(self._sweep_from))
        self._sweep.add_done_callback(_log_sweep_failure)

    def cancel(self) -> None:
        if self._sweep is not None:
            self._sweep.cancel()


def _log_sweep_failure(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.error(f"Post-restart sweep failed: {task.exception()!r}")


async def _alert_lost(finished: List[Dict[str, Any]]) -> None:
    now = time.time()
    events = []
    for q in finished:
        event = {
            "ts": now,
            "rule": "query_data_lost",
            "severity": "warning",
            "detail": f"query {q['qid']} lost {', '.join(q['missing'])}",
            "message": (
                f"Query `{q['qid']}` lost its data in a Redis restart "
                f"({json.dumps(q['missing'])} gone) and was finished as an "
                "error."
            ),
            "qid": q["qid"],
        }
        events.append(event)
        await alerts._record_alert(event)
    if len(events) == 1:
        await alerts.dispatch(events[0])
    else:
        await alerts.dispatch_batch(events, f"{len(events)} queries lost their data")
