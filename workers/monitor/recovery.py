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

Redis may be several servers, one per role or group of roles
(shepherd_utils/redis_clients.py), and each restarts on its own. Every
instance's ``run_id`` is watched, and what a restart sets off follows from
what that instance holds:

* the **data** role's instance holds the query and response payloads, so its
  restart runs the lost-payload sweep (``reconcile_lost_queries``);
* the **broker** role's instance holds the task streams, so its restart runs
  the lost-task sweep (``reconcile_lost_tasks``);
* the **lock** and **logs** roles hold nothing a sweep could restore (a merge
  lock is retaken by whoever needs it; a lost log line stays lost), so an
  instance serving only those alerts and does nothing more.

With everything on one server -- the default -- that is both sweeps on every
restart, as it always was.

Behind Sentinel, a failover also changes the ``run_id``: another server is
the primary now. It is treated the same way, because it loses data the same
way -- whatever the old primary had not replicated yet. Where the new
primary can be recognised as the old one's promoted replica the alert is
``redis_failover`` instead of ``redis_restarted``.
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

from shepherd_utils.broker import add_task, broker_client
from shepherd_utils.config import settings
from shepherd_utils.db import find_queries_with_lost_data, find_unfinished_queries

from . import alerts, redis_instances

logger = logging.getLogger("shepherd.monitor.recovery")

# The last Redis run_id the monitor saw, kept in Redis itself so a restart is
# still spotted when the monitor restarted too. A rolled-back copy of this key
# holds an older run_id, which reads as a restart -- as it should.
RUN_ID_KEY = "monitor:redis_run_id"
LOADING_POLL_SEC = 2.0
# Give up waiting for the load after this long and sweep anyway: a check made
# while Redis is still loading raises rather than reading as data loss.
LOADING_MAX_WAIT_SEC = 1800.0


async def _wait_until_loaded(wait_for: Optional[List[Any]] = None) -> None:
    """Block until Redis has finished loading its dataset after a restart.

    ``wait_for`` is the clients of the instances that restarted; the broker
    role's alone when omitted."""
    deadline = time.time() + LOADING_MAX_WAIT_SEC
    for client in [broker_client] if wait_for is None else wait_for:
        while True:
            if time.time() >= deadline:
                logger.warning(
                    "Redis still loading after restart; sweeping lost queries anyway"
                )
                return
            try:
                info = await client.info("persistence")
                if not int(info.get("loading", 0) or 0):
                    break
            except Exception as e:
                logger.debug(f"Waiting for Redis to load: {e}")
            await asyncio.sleep(LOADING_POLL_SEC)


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


# Streams whose tasks don't drive a query's workflow: a merge_message task
# merges a callback into the response, but the query only moves on when the
# lookup that is waiting on it does. A query whose only task is one of these
# has still lost its next step.
_NOT_A_NEXT_STEP = frozenset({"merge_message"})

LOST_TASK_DESCRIPTION = (
    "Shepherd's message broker restarted and lost this query's next step, so "
    "it could not be completed."
)


async def _queries_with_a_task() -> set:
    """The query ids some workflow task still refers to.

    A task stays in its stream until it is ACKed (and then deleted), so this
    covers both the tasks waiting to be picked up and the ones a worker is
    running. Every stream is read in one MULTI/EXEC, so a query being handed
    from one step's stream to the next can't be missed between two reads.
    """
    streams = [
        key
        async for key in broker_client.scan_iter(_type="stream")
        if key not in _NOT_A_NEXT_STEP
    ]
    if not streams:
        return set()
    pipe = broker_client.pipeline(transaction=True)
    for stream in streams:
        pipe.xrange(stream)
    query_ids = set()
    for entries in await pipe.execute():
        for _, fields in entries or []:
            query_id = fields.get("query_id")
            if query_id:
                query_ids.add(query_id)
    return query_ids


async def reconcile_lost_tasks(min_age_sec: float) -> List[Dict[str, Any]]:
    """Finish, as errors, unfinished queries older than ``min_age_sec`` that no
    workflow task refers to any more.

    The task streams live in the same Redis as the blobs, so a restart can
    lose a query's queued (or in-progress) task even when its blobs survived.
    Nothing would ever move such a query on. Run after ``reconcile_lost_queries``,
    whose ``finish_query`` tasks then count as the queries' next step, so no
    query is finished twice. Returns the queries it finished.
    """
    try:
        unfinished = await find_unfinished_queries(min_age_sec)
        with_a_task = await _queries_with_a_task()
    except Exception as e:
        logger.error(f"Couldn't check for queries that lost their task: {e}")
        return []
    finished: List[Dict[str, Any]] = []
    for query in unfinished:
        if query["qid"] in with_a_task:
            continue
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
                    "description": LOST_TASK_DESCRIPTION,
                    "metadata": "{}",
                },
                logger,
                raise_on_failure=True,
            )
        except Exception as e:
            logger.error(f"Couldn't hand query {query['qid']} to finish: {e}")
            continue
        finished.append({**query, "missing": ["its next task"]})
    if finished:
        logger.warning(
            f"Finished {len(finished)} queries whose task was lost: "
            f"{[q['qid'] for q in finished]}"
        )
        await _alert_lost(finished)
    return finished


async def reconcile_after_restart(
    detected_at: float,
    lost_payloads: bool = True,
    lost_tasks: bool = True,
    wait_for: Optional[List[Any]] = None,
) -> List[Dict[str, Any]]:
    """The post-restart sweep: wait for the load and the grace period, then
    finish the queries from before the restart that lost their data, and then
    the ones that lost their task.

    The defaults are a restart of the one Redis that holds everything. When
    the roles are on separate instances, ``lost_payloads`` is for a restart
    of the data role's and ``lost_tasks`` for one of the broker role's, and
    ``wait_for`` is the clients of the instances that restarted. Whatever is
    asked for runs in this one sweep, payloads first: the ``finish_query``
    tasks that step adds are what stop the task step from finishing the same
    queries again.
    """
    await _wait_until_loaded(wait_for)
    await asyncio.sleep(settings.monitor_redis_restart_grace_sec)
    min_age = time.time() - detected_at
    finished: List[Dict[str, Any]] = []
    if lost_payloads:
        finished += await reconcile_lost_queries(min_age)
    if lost_tasks:
        finished += await reconcile_lost_tasks(min_age)
    return finished


# What a restart of the instance serving each role loses, for the alert.
_ROLE_HOLDS = {
    redis_instances.BROKER: "queued tasks and worker heartbeats",
    redis_instances.DATA: "in-flight query data and received callbacks",
    "lock": "merge locks",
    "logs": "query logs",
}


def _run_id_key(view: "redis_instances.View") -> str:
    """Where an instance's last-seen run_id is kept (always in the broker
    role's Redis, with the rest of the monitor's state). The broker role's
    own keeps the key it always had."""
    return RUN_ID_KEY if view.primary else f"{RUN_ID_KEY}:{view.instance}"


def _is_failover(before: Dict[str, Any], after: Dict[str, Any]) -> bool:
    """Whether a changed run_id is a different server taking over as primary,
    rather than the same one restarting.

    A promoted replica starts a replication history of its own and keeps its
    old primary's replication id as its secondary one (``master_replid2``),
    so that the other replicas can carry on from it. The server answering now
    having, as its secondary id, the id the primary had on the last tick is
    therefore a failover. A restarted server has no such link to what it was
    before. Needs the monitor to have seen the old primary, so a failover
    while the monitor was down reads as a restart.
    """
    was = before.get("master_replid")
    return bool(was) and after.get("master_replid2") == was


class _Seen:
    """What the watcher knows about one instance."""

    def __init__(self) -> None:
        self.run_id: Optional[str] = None
        # The run_id last written to Redis. Lags ``run_id`` while Redis
        # refuses writes (it does while loading), so the write is retried.
        self.recorded: Optional[str] = None
        # The instance's INFO as of the last tick, so the restart alert can
        # say what it looked like just before it went down.
        self.last: Dict[str, Any] = {}


class RedisRestartWatcher:
    """Spots a restart (or failover) of any Redis instance from its
    ``run_id`` and starts the sweep for what that instance held."""

    def __init__(self) -> None:
        # per instance, keyed by its alert suffix (empty for the broker role's)
        self._seen: Dict[str, _Seen] = {}
        # One sweep at a time, whatever restarted: a second restart before it
        # ran -- of the same instance or another -- reschedules it to cover
        # both. Two sweeps side by side could each finish the same query.
        self._sweep: Optional[asyncio.Task] = None
        self._sweep_from = 0.0
        self._sweep_payloads = False
        self._sweep_tasks = False
        # the restarted instances the pending sweep waits on: suffix -> client
        # (None for the broker role's, which is ``broker_client``)
        self._sweep_waits: Dict[str, Any] = {}

    async def observe(self, snapshot: Dict[str, Any]) -> bool:
        """Check one poll snapshot; True when it shows a restart."""
        restarted = False
        for view in redis_instances.views(snapshot):
            if await self._observe_instance(view):
                restarted = True
        return restarted

    async def _observe_instance(self, view: "redis_instances.View") -> bool:
        redis = view.stats
        run_id = redis.get("run_id")
        if not run_id:
            return False
        seen = self._seen.setdefault(view.suffix, _Seen())
        before, seen.last = seen.last, redis
        previous = seen.run_id
        key = _run_id_key(view)
        if previous is None:
            # First tick since the monitor started: compare with the run_id
            # recorded before it did.
            try:
                previous = await broker_client.get(key)
            except Exception as e:
                logger.debug(f"Couldn't read the last Redis run_id: {e}")
                return False
        seen.run_id = run_id
        if seen.recorded != run_id:
            try:
                await broker_client.set(key, run_id)
                seen.recorded = run_id
            except Exception as e:
                logger.debug(f"Couldn't record the Redis run_id: {e}")
        if previous == run_id:
            return False
        if previous is None:
            return False  # first time this Redis has been seen at all
        await self._on_restart(view, before)
        return True

    async def _on_restart(
        self, view: "redis_instances.View", before: Dict[str, Any]
    ) -> None:
        now = time.time()
        redis = view.stats
        roles = view.roles
        lost_payloads = redis_instances.DATA in roles
        lost_tasks = redis_instances.BROKER in roles
        event = {
            "ts": now,
            "severity": "warning",
            **_restart_alert(view, before, lost_payloads, lost_tasks),
            **view.event_fields(),
        }
        logger.warning(
            f"{event['detail']}; sweeping queries that lost their data in "
            f"{settings.monitor_redis_restart_grace_sec}s"
            if lost_payloads or lost_tasks
            else f"{event['detail']}; it holds nothing to sweep for"
        )
        await alerts._record_alert(event)
        await alerts.dispatch(event)
        if not (lost_payloads or lost_tasks):
            return
        if self._sweep is not None and not self._sweep.done():
            # Restarted again -- or another instance did -- before the last
            # sweep ran: sweep once, after this restart has loaded, covering
            # queries from before either and everything either lost.
            self._sweep.cancel()
        else:
            self._sweep_from = now
            self._sweep_payloads = self._sweep_tasks = False
            self._sweep_waits = {}
        self._sweep_payloads = self._sweep_payloads or lost_payloads
        self._sweep_tasks = self._sweep_tasks or lost_tasks
        self._sweep_waits[view.suffix] = (
            None if view.primary else redis_instances.extra_client(view.instance, roles)
        )
        # Only what differs from a restart of the one shared Redis is passed,
        # so that case calls the sweep exactly as it always has.
        scope: Dict[str, Any] = {}
        if not self._sweep_payloads:
            scope["lost_payloads"] = False
        if not self._sweep_tasks:
            scope["lost_tasks"] = False
        if set(self._sweep_waits) != {""}:
            scope["wait_for"] = [
                broker_client if client is None else client
                for client in self._sweep_waits.values()
            ]
        self._sweep = asyncio.create_task(
            reconcile_after_restart(self._sweep_from, **scope)
        )
        self._sweep.add_done_callback(_log_sweep_failure)

    def cancel(self) -> None:
        if self._sweep is not None:
            self._sweep.cancel()


def _restart_alert(
    view: "redis_instances.View",
    before: Dict[str, Any],
    lost_payloads: bool,
    lost_tasks: bool,
) -> Dict[str, str]:
    """The rule, detail and message of the alert for a changed run_id."""
    redis = view.stats
    uptime = redis.get("uptime_in_seconds")
    if redis.get("aof_enabled"):
        lost = "came back from its append-only file: about the last second of writes"
    else:
        lost = "came back from its last snapshot: anything written after it"
    if not view.named and not redis.get("sentinel"):
        # The one Redis, reached directly: it can only have restarted, and
        # the alert reads as it always has.
        return {
            "rule": "redis_restarted",
            "detail": f"broker Redis restarted (up {uptime}s)",
            "message": (
                f"Broker `redis` restarted and {lost} -- in-flight query "
                "data, queued tasks, received callbacks -- is gone. Queries "
                "that lost their data will be finished as errors once it has "
                f"loaded.{_last_seen(before)}"
            ),
        }
    name = view.name("Broker `redis`")
    holds = ", ".join(_ROLE_HOLDS[role] for role in view.roles if role in _ROLE_HOLDS)
    if lost_payloads and lost_tasks:
        sweep = " Queries that lost their data or their next task"
    elif lost_payloads:
        sweep = " Queries that lost their data"
    elif lost_tasks:
        sweep = " Queries that lost their next task"
    else:
        sweep = ""
    if sweep:
        sweep += " will be finished as errors once it has loaded."
    if _is_failover(before, redis):
        return {
            "rule": view.rule("redis_failover"),
            "detail": f"Redis{view.tag} failed over to a new primary (up {uptime}s)",
            "message": (
                f"{name} failed over: one of its replicas is the primary now. "
                f"Whatever the old primary had not replicated yet -- {holds} "
                f"-- is gone.{sweep}{_last_seen(before)}"
            ),
        }
    if redis.get("sentinel"):
        # Sentinel names a primary; a new run_id is either that server
        # restarting or another one taking its place, and it can't be told
        # which (the monitor never saw the old primary, or it reported no
        # replication id).
        return {
            "rule": view.rule("redis_restarted"),
            "detail": f"Redis{view.tag} restarted or failed over (up {uptime}s)",
            "message": (
                f"{name} restarted or failed over. If it restarted, it "
                f"{lost}; if another server took over, whatever the old "
                f"primary had not replicated yet. Either way some of what it "
                f"held -- {holds} -- is gone.{sweep}{_last_seen(before)}"
            ),
        }
    return {
        "rule": view.rule("redis_restarted"),
        "detail": f"Redis{view.tag} restarted (up {uptime}s)",
        "message": (
            f"{name} restarted and {lost} -- {holds} -- is gone.{sweep}"
            f"{_last_seen(before)}"
        ),
    }


def _gb(n: Any) -> str:
    return f"{int(n or 0) / 2**30:.1f} GB"


def _last_seen(redis: Dict[str, Any]) -> str:
    """What the instance looked like on the monitor's last tick before the
    restart -- Redis can't say why it died, but a footprint near the container
    limit or a fork in progress points at an OOM kill."""
    if not redis or "error" in redis:
        return ""
    parts = [f"data {_gb(redis.get('used_memory_bytes'))}"]
    if redis.get("maxmemory_pct") is not None:
        parts[0] += f" ({redis['maxmemory_pct']}% of maxmemory)"
    footprint = f"footprint {_gb(redis.get('footprint_bytes'))}"
    if redis.get("memory_limit_pct") is not None:
        footprint += f" ({redis['memory_limit_pct']}% of the container limit)"
    parts.append(footprint)
    if redis.get("fork_in_progress"):
        parts.append("a snapshot/AOF-rewrite fork was running")
    return f" Last seen before the restart: {', '.join(parts)}."


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
                f"Query `{q['qid']}` lost {json.dumps(q['missing'])} in a "
                "Redis restart and was finished as an error."
            ),
            "qid": q["qid"],
        }
        events.append(event)
        await alerts._record_alert(event)
    if len(events) == 1:
        await alerts.dispatch(events[0])
    else:
        await alerts.dispatch_batch(events, f"{len(events)} queries lost their data")
