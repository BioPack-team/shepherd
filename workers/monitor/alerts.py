"""Alert rule evaluation and dispatch.

Rules are loaded from a YAML file (path configured via ``MONITOR_ALERTS_CONFIG``).
On every snapshot the engine evaluates each rule against the snapshot. A rule
that has been firing continuously for at least ``duration`` seconds emits an
alert. Cooldown state (``alert:cooldown:{rule}``) lives in Redis so we don't
re-notify on every tick.

Redis is watched per instance (see ``redis_instances``): by default there is
one, and its alerts are named as they always were (``redis_memory_high``).
When roles have been moved onto Redis servers of their own, every instance is
evaluated with its own state, and the alerts of an instance other than the
broker role's carry its name (``redis_memory_high@shepherd_data:6379``).
"""

import asyncio
import json
import logging
import smtplib
import ssl
import time
from dataclasses import dataclass, field
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
import yaml

from shepherd_utils.broker import broker_client
from shepherd_utils.config import settings

from . import redis_instances

logger = logging.getLogger("shepherd.monitor.alerts")

ALERT_HISTORY_KEY = "monitor:alerts:history"
ALERT_HISTORY_LIMIT = 200


def _parse_duration(value: Any) -> float:
    """Accept ``60``, ``"60s"``, ``"5m"``, ``"1h"``."""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().lower()
    if s.endswith("ms"):
        return float(s[:-2]) / 1000
    if s.endswith("s"):
        return float(s[:-1])
    if s.endswith("m"):
        return float(s[:-1]) * 60
    if s.endswith("h"):
        return float(s[:-1]) * 3600
    return float(s)


class Rule:
    def __init__(self, raw: Dict[str, Any]):
        self.name = raw["name"]
        self.kind = raw.get("type", "threshold")
        self.metric = raw.get("metric")
        self.stream = raw.get("stream")
        self.worker = raw.get("worker")
        self.threshold = raw.get("threshold")
        # stuck_pending only: how long a consumer's held-but-unacked tasks must
        # sit idle before it counts as wedged. Milliseconds, to match XINFO.
        self.idle_ms = raw.get("idle_ms")
        self.duration = _parse_duration(raw.get("duration", 0))
        self.cooldown = _parse_duration(raw.get("cooldown", "10m"))
        self.severity = raw.get("severity", "warning")
        self.message = raw.get("message")
        # Per-rule transient state. Tracks first time the rule started firing
        # in the current "violation streak" so we can require ``duration`` of
        # continuous breach before alerting.
        self._first_fired_at: Optional[float] = None
        # The same streak for each further thing a rule watches separately
        # (a ``redis_eviction`` rule watches every Redis instance), keyed by
        # the scope ``checks`` gives it. One instance's streak ending must not
        # end another's.
        self._scoped_first_fired_at: Dict[str, float] = {}

    def reset_streak(self, scope: str = "") -> None:
        if scope:
            self._scoped_first_fired_at.pop(scope, None)
        else:
            self._first_fired_at = None

    def streak_started_at(self, now: float, scope: str = "") -> float:
        """When the current breach of ``scope`` began (now, if it just did)."""
        if scope:
            return self._scoped_first_fired_at.setdefault(scope, now)
        if self._first_fired_at is None:
            self._first_fired_at = now
        return self._first_fired_at

    def checks(
        self, snapshot: Dict[str, Any]
    ) -> List[Tuple[str, str, Optional[str], Optional[str], Dict[str, Any]]]:
        """Everything this rule evaluates on a snapshot, as ``(scope, alert
        name, detail, message, extra event fields)``; ``detail`` is None where
        the rule isn't breached.

        Nearly every rule is one check under its own name. A ``redis_eviction``
        rule is one per Redis instance: the broker role's under the rule's
        name, as ever, and each other instance under ``<name>@<instance>``
        with a streak and a cooldown of its own, so eviction on the data Redis
        alerts even while the broker's alert is in its cooldown (and the
        other way round).
        """
        if self.kind != "redis_eviction":
            detail = self.evaluate(snapshot)
            return [("", self.name, detail, self.message or detail, {})]
        found = []
        for view in redis_instances.views(snapshot):
            detail = self._redis_eviction_detail(view)
            message = self.message or detail
            if detail and self.message and view.named:
                # a configured message can't know which instance it is about
                message = f"{self.message} ({detail})"
            found.append(
                (
                    view.suffix,
                    view.rule(self.name),
                    detail,
                    message,
                    view.event_fields(),
                )
            )
        return found

    def evaluate(self, snapshot: Dict[str, Any]) -> Optional[str]:
        """Return a human-readable detail string if the rule is currently breached."""
        if self.kind == "threshold":
            return self._eval_threshold(snapshot)
        if self.kind == "heartbeat_lost":
            return self._eval_heartbeat_lost(snapshot)
        if self.kind == "oldest_callback_age":
            age = snapshot["postgres"].get("oldest_callback_age_sec", 0)
            if self.threshold is not None and age > float(self.threshold):
                return f"oldest callback age {age:.0f}s exceeds {self.threshold}s"
            return None
        if self.kind == "queue_pending":
            stats = snapshot["streams"].get(self.stream)
            if (
                stats
                and self.threshold is not None
                and stats["pending"] > self.threshold
            ):
                return f"{self.stream} pending {stats['pending']} > {self.threshold}"
            return None
        if self.kind == "db_capacity":
            return self._eval_db_capacity(snapshot)
        if self.kind == "payload_store_capacity":
            return self._eval_payload_store_capacity(snapshot)
        if self.kind == "redis_eviction":
            return self._eval_redis_eviction(snapshot)
        if self.kind == "stuck_pending":
            return self._eval_stuck_pending(snapshot)
        return None

    def _eval_threshold(self, snapshot: Dict[str, Any]) -> Optional[str]:
        if self.metric == "xlen" and self.stream:
            stats = snapshot["streams"].get(self.stream)
            if not stats:
                return None
            if self.threshold is not None and stats["xlen"] > self.threshold:
                return f"{self.stream} xlen {stats['xlen']} > {self.threshold}"
        elif self.metric == "callbacks_pending":
            v = snapshot["postgres"].get("callbacks_pending", 0)
            if self.threshold is not None and v > self.threshold:
                return f"callbacks_pending {v} > {self.threshold}"
        elif self.metric == "pg_connection_count":
            v = snapshot["postgres"].get("connection_count", 0)
            if self.threshold is not None and v > self.threshold:
                return f"pg connections {v} > {self.threshold}"
        elif self.metric == "pg_connection_pct":
            # Client backends as a percentage of what Postgres accepts, so the
            # rule tracks max_connections instead of a hand-counted fleet.
            pg = snapshot["postgres"]
            usable = pg.get("usable_connections") or 0
            if usable <= 0 or self.threshold is None:
                return None
            used = pg.get("client_connections", 0)
            pct = 100.0 * used / usable
            if pct > self.threshold:
                return (
                    f"pg connections {used}/{usable} ({pct:.0f}%) > "
                    f"{self.threshold:g}% of max_connections"
                )
        return None

    def _eval_payload_store_capacity(self, snapshot: Dict[str, Any]) -> Optional[str]:
        """Fire when the ARS payload volume is ``threshold`` percent full.

        The watchdog evicts finished trees from ``ars_payload_evict_high_pct``
        down to the low mark, so a threshold above the high mark only trips
        when eviction can't keep up: a burst faster than a watchdog pass, or
        a volume filled by retained and still-running trees, which are never
        evicted. A no-op when the watchdog isn't publishing (no ARS, or
        payloads still kept in Postgres).
        """
        store = snapshot.get("ars_payload_store") or {}
        pct = store.get("used_pct")
        if pct is None or self.threshold is None or pct < float(self.threshold):
            return None
        detail = (
            f"ARS payload volume {pct:.1f}% full "
            f"({store.get('used_bytes', 0) / 1e9:.1f} of "
            f"{store.get('total_bytes', 0) / 1e9:.1f} GB)"
        )
        if store.get("retained_pct") is not None:
            detail += f", {store['retained_pct']:.1f}% retained trees"
        if store.get("short_bytes"):
            detail += (
                f"; eviction is {store['short_bytes'] / 1e9:.2f} GB short "
                "with nothing left it may evict"
            )
        return detail

    def _eval_db_capacity(self, snapshot: Dict[str, Any]) -> Optional[str]:
        """Fire when the Postgres volume crosses ``threshold`` percent full.

        ``disk_used_pct`` is populated by the poller only when
        ``PG_VOLUME_CAPACITY`` is configured; without that we don't know the
        denominator, so the rule is a no-op. The percentage is based on
        ``pg_database_size`` + WAL, which under-counts true filesystem use --
        early warning, not a substitute for an infra-level volume alert.
        """
        pg = snapshot.get("postgres", {})
        capacity = pg.get("disk_capacity_bytes", 0)
        if not capacity or self.threshold is None:
            return None
        pct = pg.get("disk_used_pct", 0)
        if pct >= float(self.threshold):
            used_gb = pg.get("disk_used_bytes", 0) / 1e9
            cap_gb = capacity / 1e9
            return (
                f"Postgres volume {pct:.1f}% full "
                f"({used_gb:.1f}GB of {cap_gb:.1f}GB) >= {self.threshold}%"
            )
        return None

    def _eval_redis_eviction(self, snapshot: Dict[str, Any]) -> Optional[str]:
        """Fire when the broker evicted keys in the last interval.

        With the ``volatile-ttl`` policy, eviction only happens once the broker
        is *at* its maxmemory cap, so a non-zero delta is a definitive "the cap
        has been reached and blobs are being shed" signal -- distinct from the
        percentage warning above, which is a lead indicator. ``threshold``
        defaults to 0 (any eviction); raise it to ignore trivial churn.

        This is the broker role's instance; ``checks`` covers the others.
        """
        return self._redis_eviction_detail(redis_instances.primary_view(snapshot))

    def _redis_eviction_detail(self, view: "redis_instances.View") -> Optional[str]:
        """The eviction breach of one Redis instance, or None."""
        delta = view.stats.get("evicted_keys_delta", 0) or 0
        floor = float(self.threshold) if self.threshold is not None else 0.0
        if delta <= floor:
            return None
        if view.named:
            return (
                f"Redis{view.tag} evicted {delta} key(s) in the last interval "
                "-- it is at its maxmemory cap and shedding data"
            )
        return (
            f"Redis evicted {delta} key(s) in the last interval -- broker is "
            "at its maxmemory cap and shedding data"
        )

    def _eval_stuck_pending(self, snapshot: Dict[str, Any]) -> Optional[str]:
        """Fire when a consumer is holding tasks it hasn't acked for too long.

        This catches a *wedged* worker: one whose heartbeat may still be live
        (so ``heartbeat_lost`` never trips) but which grabbed stream messages
        and then stopped processing -- they sit in its pending-entries list,
        idle, until reclaim eventually rescues them. Distinct from a plain
        backlog (``xlen`` high but actively draining). Scans every stream when
        no ``stream`` is named, so one rule covers the whole fleet.
        """
        streams = snapshot.get("streams", {})
        targets = [self.stream] if self.stream else list(streams.keys())
        min_pending = int(self.threshold) if self.threshold is not None else 1
        idle_ms = float(self.idle_ms) if self.idle_ms is not None else 60000.0
        stuck: List[str] = []
        for name in targets:
            stats = streams.get(name) or {}
            for c in stats.get("consumers", []):
                if (c.get("pending", 0) or 0) >= min_pending and (
                    c.get("idle_ms", 0) or 0
                ) >= idle_ms:
                    stuck.append(
                        f"{name}/{c.get('name')} "
                        f"({c.get('pending')} pending, idle {c.get('idle_ms', 0) / 1000:.0f}s)"
                    )
        if stuck:
            return "wedged consumers holding unacked tasks: " + ", ".join(stuck)
        return None

    def _eval_heartbeat_lost(self, snapshot: Dict[str, Any]) -> Optional[str]:
        # Fires when a worker type drops to zero alive OR has any stale members.
        if self.worker:
            info = snapshot["workers"].get(self.worker)
            if info is None or info["alive"] == 0:
                return f"worker {self.worker} has no live heartbeats"
            if info["stale"] > 0:
                return f"worker {self.worker} has {info['stale']} stale heartbeats"
            return None
        # If no worker named, fire on any worker type that vanished after we'd
        # previously seen it (handled at the engine level via events).
        return None


def load_rules(path: str) -> List[Rule]:
    p = Path(path)
    if not p.exists():
        logger.warning(f"Alert config {path} not found; running with no rules")
        return []
    try:
        data = yaml.safe_load(p.read_text()) or {}
    except Exception as e:
        logger.error(f"Failed to parse alert config {path}: {e}")
        return []
    rules_raw = data.get("rules", [])
    rules: List[Rule] = []
    for raw in rules_raw:
        try:
            rules.append(Rule(raw))
        except KeyError as e:
            logger.error(f"Skipping malformed rule {raw}: missing {e}")
    logger.info(f"Loaded {len(rules)} alert rules from {path}")
    return rules


def _persistence_failures(redis: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    """What is wrong with an instance's persistence, as ``(what, what it
    means, severity)``. The three failures INFO reports are not equally bad,
    and the alert has to say which one it is (#201):

    * a failed AOF *write* is the one that loses data: Redis is accepting
      writes it is not putting on disk;
    * a failed AOF *rewrite* (the background compaction) loses nothing: the
      existing file is still being appended to, it just keeps growing until
      a rewrite succeeds or the disk fills;
    * a failed RDB snapshot leaves the last good snapshot as what a restart
      comes back from -- which only matters without an AOF -- and makes Redis
      refuse writes while ``stop-writes-on-bgsave-error`` is on (its default).
    """
    failures = []
    if redis.get("aof_last_write_status") == "err":
        failures.append(
            (
                "AOF write failing",
                "writes are not being persisted, so a restart loses everything "
                "written since the last successful write",
                "critical",
            )
        )
    if redis.get("aof_last_bgrewrite_status") == "err":
        failures.append(
            (
                "AOF rewrite failed",
                "no data lost yet, but the AOF will keep growing until a "
                "rewrite succeeds",
                "warning",
            )
        )
    if redis.get("rdb_last_bgsave_status") == "err":
        if redis.get("aof_enabled"):
            means = "the append-only file still holds the data"
        else:
            means = "a restart loses everything written since the last good snapshot"
        failures.append(
            (
                "RDB snapshot failed",
                f"{means}, and Redis refuses writes while this lasts if "
                "stop-writes-on-bgsave-error is on (its default)",
                "critical",
            )
        )
    return failures


_SEVERITY_RANK = {"warning": 1, "critical": 2}


class _Held:
    """A yes/no condition that only counts once it has held for a grace
    period, in both directions: on for the grace before it alerts, off for
    the grace before it clears. One per alert per Redis instance."""

    def __init__(self) -> None:
        self.active = False
        self._changed_at: Optional[float] = None

    def update(self, observed: bool, now: float, grace: float) -> Optional[bool]:
        """Feed one observation. Returns True on the tick the condition
        becomes active, False on the tick it clears, None otherwise."""
        if observed == self.active:
            self._changed_at = None
            return None
        if self._changed_at is None:
            self._changed_at = now
        if (now - self._changed_at) < grace:
            return None
        self.active = observed
        self._changed_at = None
        return observed


@dataclass
class _MemoryState:
    """One Redis instance's memory-pressure state (see handle_redis_memory)."""

    level: str = "ok"
    candidate: str = "ok"
    candidate_since: float = 0.0
    last_alerted_at: float = 0.0


@dataclass
class _ReplicationState:
    """One Redis instance's replication state (see handle_redis_replication)."""

    # The most connected replicas this instance has had since the monitor
    # started. 0 means it has never had one, and nothing here alerts.
    expected: int = 0
    lost: _Held = field(default_factory=_Held)
    # Whether the current loss has already alerted as total (critical), so a
    # partial loss that becomes total alerts once more.
    lost_all_alerted: bool = False
    lagging: _Held = field(default_factory=_Held)


def eviction_window_min_sec() -> float:
    """The shortest eviction window that is still safe: the longest a query
    may run, times a safety factor, unless configured outright. A payload has
    to outlive the query that wrote it; below this, a capped instance evicts
    the data of queries that are still running."""
    configured = settings.monitor_redis_eviction_window_min_sec
    if configured > 0:
        return configured
    return (
        settings.ars_timeout_merge_sec
        * settings.monitor_redis_eviction_window_safety_factor
    )


def _minutes(seconds: float) -> str:
    return f"{seconds / 60:.1f} min"


def _redis_memory_usage(redis: Dict[str, Any]) -> Optional[Tuple[float, str]]:
    """The broker's memory use as (percent, what it's a percent of), taking
    the higher of data vs. ``maxmemory`` and footprint vs. the container limit.
    None when neither is known."""
    candidates = []
    maxmem = int(redis.get("maxmemory_bytes", 0) or 0)
    if maxmem > 0:
        used = int(redis.get("used_memory_bytes", 0) or 0)
        candidates.append((100.0 * used / maxmem, "maxmemory"))
    limit = int(redis.get("memory_limit_bytes", 0) or 0)
    if limit > 0:
        footprint = int(redis.get("footprint_bytes", 0) or 0)
        candidates.append((100.0 * footprint / limit, "container memory limit"))
    return max(candidates) if candidates else None


def _lagging_replicas(redis: Dict[str, Any]) -> List[str]:
    """The connected replicas of an instance that are not keeping up, each
    described for the alert."""
    max_sec = settings.monitor_redis_replica_lag_sec
    max_bytes = settings.monitor_redis_replica_lag_bytes
    behind = []
    for replica in redis.get("replicas") or []:
        lag_sec = replica.get("lag_sec") or 0
        lag_bytes = replica.get("lag_bytes") or 0
        if replica.get("state") != "online":
            # connected, but still receiving its initial copy of the dataset
            why = f"state {replica.get('state')}"
        elif lag_sec > max_sec:
            why = f"last heard from {lag_sec}s ago"
        elif max_bytes and lag_bytes > max_bytes:
            why = f"{lag_bytes / 2**20:.0f} MiB behind"
        else:
            continue
        behind.append(f"{replica.get('addr')} {why}")
    return behind


class AlertEngine:
    def __init__(self, rules: List[Rule]):
        self.rules = rules
        # Captured once at process boot. Worker-down alerts that fire during
        # the startup grace window get suppressed so a fresh ``docker compose
        # up`` doesn't immediately spam Slack while workers are still booting.
        self._boot_time = time.time()
        # Worker-down alerts (crash/scale-down transitions and heartbeat_lost
        # rules) are buffered here keyed by worker name rather than dispatched
        # immediately. When a laptop sleeps, every worker drops to zero at once;
        # buffering for ``monitor_down_debounce_sec`` lets us send a single
        # Slack/email message listing all of them instead of ~20 separate ones.
        # The buffer flushes on a normal poll tick once the window elapses.
        self._down_buffer: Dict[str, Dict[str, Any]] = {}
        self._down_buffer_started_at: float = 0.0
        # Broker-availability state machine. Driven once per poll tick by
        # ``handle_broker_health``. We track it in-process (not via a Redis
        # cooldown key) precisely because the broker is unreachable when it
        # matters -- a Redis-backed guard couldn't be written then. ``_broker_up``
        # is the last-observed state; ``_broker_down_since`` debounces transient
        # blips before we alert; ``_broker_down_alerted`` makes the down-alert
        # fire once per outage; ``_broker_recovered_at`` opens the grace window
        # during which the post-restart worker-down flood is suppressed.
        self._broker_up: bool = True
        self._broker_down_since: float = 0.0
        self._broker_down_alerted: bool = False
        self._broker_recovered_at: float = 0.0
        # Redis-eviction grace window. When the broker evicts keys under memory
        # pressure it can shed live workers' short-TTL heartbeat keys, making
        # them read as zero-alive though they never disconnected. This holds the
        # wall-clock time until which worker-down alerts stay suppressed; it's
        # pushed forward on every tick that observes an eviction, then lets the
        # window elapse so heartbeats can re-register before we trust "down".
        self._redis_eviction_grace_until: float = 0.0
        # Postgres-availability state machine. Same debounce/fire-once shape as
        # the broker's, minus the recovery-grace window (a PG outage doesn't
        # zero out heartbeats, so there's no worker-down flood to suppress).
        self._pg_up: bool = True
        self._pg_down_since: float = 0.0
        self._pg_down_alerted: bool = False
        # Redis memory-pressure state machine (see handle_redis_memory). Like the
        # broker/postgres machines it's driven once per poll tick and tracked
        # in-process. It tiers usage into ok/warning/critical so only the current
        # level alerts (crossing the critical line doesn't also fire the warning),
        # re-notifies at most once per cooldown while elevated, and emits a
        # recovery alert when usage returns below the warning threshold.
        # ``_candidate``/``_since`` debounce a level change so a single-tick spike
        # (or dip) doesn't trip it.
        # All of it is kept per Redis instance (keyed by the instance's alert
        # suffix, empty for the broker role's), so one instance's level, grace
        # and cooldown never mask or reset another's.
        self._redis_mem: Dict[str, _MemoryState] = {}
        # The severity rank (0 = fine) of each instance's persistence failures
        # as of the last snapshot (see handle_redis_persistence).
        self._redis_persistence: Dict[str, int] = {}
        # Per-instance replication state (see handle_redis_replication) and
        # whether its eviction window is too short (handle_redis_eviction_window).
        self._redis_replication: Dict[str, _ReplicationState] = {}
        self._redis_eviction_window: Dict[str, _Held] = {}

    @property
    def in_startup_grace(self) -> bool:
        return (time.time() - self._boot_time) < settings.monitor_startup_grace_sec

    @property
    def in_broker_recovery_grace(self) -> bool:
        """True while we're inside the post-recovery window (or broker is down).

        Worker-down alerts are suppressed in this window because a broker
        restart transiently zeroes every worker's heartbeats until they
        re-register -- that's not the workers dying, it's the broker having
        just come back.
        """
        if not self._broker_up:
            return True
        if not self._broker_recovered_at:
            return False
        return (
            time.time() - self._broker_recovered_at
        ) < settings.monitor_broker_recovery_grace_sec

    @property
    def in_redis_eviction_grace(self) -> bool:
        """True while (or shortly after) the broker is evicting keys.

        Redis key eviction removes keys to free memory; it does *not* close
        client connections, so the workers themselves are fine. But the monitor
        infers worker liveness from short-TTL heartbeat keys, which eviction can
        shed -- so during and just after an eviction spell a live worker can read
        as zero-alive. Suppressing worker-down alerts across this window keeps a
        Redis memory spike from spamming bogus worker-crash notifications.
        """
        return time.time() < self._redis_eviction_grace_until

    def _note_redis_eviction(self, snapshot: Dict[str, Any]) -> None:
        """Open/extend the eviction grace window when this tick saw evictions.

        Only the broker role's instance counts: the heartbeat keys live there,
        so another instance evicting cannot make a worker read as down."""
        redis = redis_instances.primary_view(snapshot).stats
        try:
            delta = int(redis.get("evicted_keys_delta", 0) or 0)
        except (TypeError, ValueError):
            delta = 0
        if delta > 0:
            self._redis_eviction_grace_until = (
                time.time() + settings.monitor_redis_eviction_grace_sec
            )

    async def handle_broker_health(self, is_up: bool) -> None:
        """Drive the broker up/down state machine and emit broker alerts.

        Called every poll tick with the result of ``poller.probe_broker()``.
        Fires exactly one ``broker_down`` alert per outage (after a debounce
        window) and one ``broker_recovered`` alert when it returns.
        """
        now = time.time()
        if not is_up:
            if self._broker_up:
                # Healthy -> down edge: start the debounce clock.
                self._broker_up = False
                self._broker_down_since = now
            elif (
                not self._broker_down_alerted
                and (now - self._broker_down_since)
                >= settings.monitor_broker_down_grace_sec
            ):
                # Sustained outage: fire once. Slack/email dispatch does not go
                # through the broker, so this gets out even while Redis is down;
                # the Redis-backed history write inside _record_alert degrades
                # gracefully and the Postgres archive still lands.
                self._broker_down_alerted = True
                event = {
                    "ts": now,
                    "rule": "broker_down",
                    "severity": "critical",
                    "detail": "broker unreachable (PING failed)",
                    "message": (
                        "Broker `redis` is unreachable -- workers cannot read or "
                        "write tasks. Worker-down alerts are suppressed until it "
                        "recovers so this outage isn't buried under a flood of "
                        "derived worker-crash alerts."
                    ),
                }
                await _record_alert(event)
                await dispatch(event)
            return
        # is_up
        if not self._broker_up:
            # Down -> up edge: open the recovery grace window and announce it.
            self._broker_up = True
            self._broker_recovered_at = now
            was_alerted = self._broker_down_alerted
            self._broker_down_since = 0.0
            self._broker_down_alerted = False
            # Only announce recovery if we actually announced the outage, so a
            # sub-debounce blip stays silent on both edges.
            if was_alerted:
                event = {
                    "ts": now,
                    "rule": "broker_recovered",
                    "severity": "info",
                    "detail": "broker answering PING again",
                    "message": (
                        "Broker `redis` is reachable again. Worker-down alerts "
                        f"stay suppressed for {settings.monitor_broker_recovery_grace_sec}s "
                        "while workers re-register their heartbeats."
                    ),
                }
                await _record_alert(event)
                await dispatch(event)

    async def handle_postgres_health(self, is_up: bool) -> None:
        """Drive the Postgres up/down state machine and emit its alerts.

        Called every poll tick with whether the latest snapshot reached
        Postgres. Fires one ``postgres_down`` alert per outage (after a debounce
        window) and one ``postgres_recovered`` when it returns. Unlike the
        broker, the alert path (Redis cooldowns, Slack/email) doesn't depend on
        Postgres, so ordinary dispatch works throughout the outage.
        """
        now = time.time()
        if not is_up:
            if self._pg_up:
                self._pg_up = False
                self._pg_down_since = now
            elif (
                not self._pg_down_alerted
                and (now - self._pg_down_since)
                >= settings.monitor_postgres_down_grace_sec
            ):
                self._pg_down_alerted = True
                event = {
                    "ts": now,
                    "rule": "postgres_down",
                    "severity": "critical",
                    "detail": "postgres unreachable",
                    "message": (
                        "Postgres is unreachable -- durable query state can't be "
                        "read or written, and the janitor can't purge or reap "
                        "abandoned queries. Redis-backed task flow may still run, "
                        "so this can be silent without this alert."
                    ),
                }
                await _record_alert(event)
                await dispatch(event)
            return
        # is_up
        if not self._pg_up:
            self._pg_up = True
            was_alerted = self._pg_down_alerted
            self._pg_down_since = 0.0
            self._pg_down_alerted = False
            if was_alerted:
                event = {
                    "ts": now,
                    "rule": "postgres_recovered",
                    "severity": "info",
                    "detail": "postgres reachable again",
                    "message": "Postgres is reachable again.",
                }
                await _record_alert(event)
                await dispatch(event)

    async def handle_redis_memory(self, snapshot: Dict[str, Any]) -> None:
        """Drive the Redis memory-pressure state machine and emit its alerts.

        Modeled on the broker/postgres health machines rather than run as two
        independent threshold rules, so three behaviors compose cleanly:

        * **Tiering without duplicates**: usage is classified into a single
          level -- ok (< warning%), warning (>= warning%), or critical
          (>= critical%). Only the current level ever alerts, so crossing the
          critical line does NOT also send the warning alert.
        * **Day-long backoff**: while usage stays elevated we re-notify at most
          once per ``monitor_redis_memory_cooldown_sec`` (a full day by default)
          instead of every few minutes.
        * **Recovery**: when usage falls back below the warning threshold we send
          a one-off "recovered" alert, just like the broker/postgres machines.
        * **No debounce at the brink**: at ``monitor_redis_memory_immediate_pct``
          the critical alert fires on the first tick. A burst can take the
          broker from fine to OOM-killed well inside the grace window.

        Usage is the higher of two ratios: the data (``used_memory``) against
        ``maxmemory``, and the container footprint (RSS plus fork copy-on-write)
        against the container memory limit (``redis_memory_limit``). The second
        is what the kernel OOM-kills on, and it can run out first. A no-op when
        neither denominator is known (``maxmemory`` 0 and no limit configured).

        Every Redis instance runs its own copy of the machine.
        """
        now = time.time()
        for view in redis_instances.views(snapshot):
            await self._redis_memory_tick(view, now)

    async def _redis_memory_tick(
        self, view: "redis_instances.View", now: float
    ) -> None:
        """One instance's step of the memory-pressure machine."""
        if view.named and "error" in view.stats:
            # This instance didn't answer (the others did). Not knowing isn't
            # a recovery: hold its state until it can be read again.
            return
        state = self._redis_mem.setdefault(view.suffix, _MemoryState())
        usage = _redis_memory_usage(view.stats)
        if usage is None:
            # Uncapped: reset to ok silently. Losing the denominator isn't a real
            # recovery, so don't announce one.
            state.level = "ok"
            state.candidate = "ok"
            return
        pct, basis = usage
        if (
            pct >= settings.monitor_redis_memory_immediate_pct
            and state.level != "critical"
        ):
            state.level = "critical"
            state.candidate = "critical"
            state.candidate_since = now
            await self._fire_redis_memory(view, state, "critical", pct, now, basis)
            return
        if pct >= settings.monitor_redis_memory_critical_pct:
            target = "critical"
        elif pct >= settings.monitor_redis_memory_warning_pct:
            target = "warning"
        else:
            target = "ok"

        # Debounce: an observed level must hold for the grace window before we
        # act on it, so a single-tick spike (or dip) doesn't alert or clear.
        if target != state.candidate:
            state.candidate = target
            state.candidate_since = now
            return
        if target == state.level:
            # Sustained at the committed level. Re-notify an elevated level at
            # most once per cooldown; ``ok`` needs nothing.
            if (
                target != "ok"
                and (now - state.last_alerted_at)
                >= settings.monitor_redis_memory_cooldown_sec
            ):
                await self._fire_redis_memory(view, state, target, pct, now, basis)
            return
        # A different level has held steady long enough: commit the transition.
        if (now - state.candidate_since) < settings.monitor_redis_memory_grace_sec:
            return
        prev = state.level
        state.level = target
        if target == "ok":
            # Recovery: an elevated committed level is only ever reached by
            # alerting, so a return to ok is always worth an "it's better" note.
            await self._fire_redis_memory_recovery(view, pct, now, basis)
        elif target == "critical" or prev == "ok":
            # Fire when entering an elevated level from ok, or escalating to
            # critical. A critical->warning de-escalation stays quiet (still
            # elevated and already alerted) until it either clears or re-escalates.
            await self._fire_redis_memory(view, state, target, pct, now, basis)

    async def _fire_redis_memory(
        self,
        view: "redis_instances.View",
        state: _MemoryState,
        level: str,
        pct: float,
        now: float,
        basis: str = "maxmemory",
    ) -> None:
        """Dispatch (and record) a Redis memory warning/critical alert."""
        state.last_alerted_at = now
        if level == "critical":
            rule = "redis_memory_critical"
            severity = "critical"
            threshold = settings.monitor_redis_memory_critical_pct
            tail = "Eviction of live data is imminent"
        else:
            rule = "redis_memory_high"
            severity = "warning"
            threshold = settings.monitor_redis_memory_warning_pct
            tail = "Eviction and OOM-kill risk are near"
        event = {
            "ts": now,
            "rule": view.rule(rule),
            "severity": severity,
            "detail": (
                f"Redis{view.tag} memory {pct:.1f}% of {basis} "
                f"(>= {threshold:.0f}%)"
            ),
            "message": (
                f"{view.name('Redis broker')} memory is at {pct:.1f}% of its "
                f"{basis} (>= {threshold:.0f}%). {tail} -- shed load, raise the "
                "cap, or lower redis_ttl."
            ),
            **view.event_fields(),
        }
        await _record_alert(event)
        await dispatch(event)

    async def _fire_redis_memory_recovery(
        self,
        view: "redis_instances.View",
        pct: float,
        now: float,
        basis: str = "maxmemory",
    ) -> None:
        """Dispatch (and record) the Redis memory 'recovered' alert."""
        warn = settings.monitor_redis_memory_warning_pct
        event = {
            "ts": now,
            "rule": view.rule("redis_memory_recovered"),
            "severity": "info",
            "detail": f"Redis{view.tag} memory back to {pct:.1f}% of {basis}",
            "message": (
                f"{view.name('Redis broker')} memory has fallen back below "
                f"{warn:.0f}% of its {basis} (now {pct:.1f}%). Memory pressure "
                "has cleared."
            ),
            **view.event_fields(),
        }
        await _record_alert(event)
        await dispatch(event)

    async def handle_redis_persistence(self, snapshot: Dict[str, Any]) -> None:
        """Alert when a Redis instance's snapshot or append-only file stops
        saving, saying which and what it costs (``_persistence_failures``).

        A failing save is silent until the next restart, which then loses
        everything since the last good one. One ``redis_persistence_failing``
        alert when a save first reports an error -- critical when data is at
        risk (a failed AOF write or RDB snapshot), warning when it is not yet
        (a failed AOF rewrite alone) -- another if it gets worse, and one
        ``redis_persistence_recovered`` when they all succeed again.
        """
        now = time.time()
        for view in redis_instances.views(snapshot):
            redis = view.stats
            if "error" in redis:
                continue
            failures = _persistence_failures(redis)
            severity = max(
                (sev for _, _, sev in failures),
                key=_SEVERITY_RANK.__getitem__,
                default=None,
            )
            rank = _SEVERITY_RANK.get(severity, 0)
            was = self._redis_persistence.get(view.suffix, 0)
            self._redis_persistence[view.suffix] = rank
            if rank > was:
                what = ", ".join(name for name, _, _ in failures)
                event = {
                    "ts": now,
                    "rule": view.rule("redis_persistence_failing"),
                    "severity": severity,
                    "detail": f"Redis{view.tag} {what}",
                    "message": (
                        f"{view.name('Broker `redis`')} persistence: "
                        + "; ".join(f"{name}: {means}" for name, means, _ in failures)
                        + ". Check its log and that its data volume has free space."
                    ),
                    **view.event_fields(),
                }
            elif was and not rank:
                event = {
                    "ts": now,
                    "rule": view.rule("redis_persistence_recovered"),
                    "severity": "info",
                    "detail": f"Redis{view.tag} saving to disk again",
                    "message": (
                        f"{view.name('Broker `redis`')} persistence is "
                        "succeeding again."
                    ),
                    **view.event_fields(),
                }
            else:
                continue
            await _record_alert(event)
            await dispatch(event)

    async def handle_redis_replication(self, snapshot: Dict[str, Any]) -> None:
        """Alert when a Redis instance loses replicas or its replicas fall
        behind (#201).

        A primary whose replica is gone or far behind still serves every
        request, so nothing else notices -- until it fails, and the failover
        (or the restart) loses whatever the replica never received. Per
        instance, both debounced by ``monitor_redis_replica_grace_sec`` in
        each direction:

        * ``redis_replicas_lost`` when it has fewer connected replicas than it
          has had (critical at zero: no redundancy left), and
          ``redis_replicas_recovered`` when they are all back;
        * ``redis_replica_lagging`` when a connected replica is still syncing,
          silent for ``monitor_redis_replica_lag_sec``, or more than
          ``monitor_redis_replica_lag_bytes`` behind, and
          ``redis_replica_caught_up`` when none is.

        "Has had" is the most replicas seen since the monitor started, so an
        instance that never had one (the single-server default) never alerts,
        and a deliberate scale-down alerts once and is the new normal after
        the monitor next restarts.
        """
        now = time.time()
        grace = settings.monitor_redis_replica_grace_sec
        for view in redis_instances.views(snapshot):
            redis = view.stats
            # Only a primary's replica list means anything: a replica reports
            # zero replicas of its own. Sentinel always connects to the primary.
            if "error" in redis or redis.get("role") != "master":
                continue
            connected = int(redis.get("connected_replicas") or 0)
            state = self._redis_replication.setdefault(view.suffix, _ReplicationState())
            state.expected = max(state.expected, connected)
            if not state.expected:
                continue
            events = []
            changed = state.lost.update(connected < state.expected, now, grace)
            none_left = connected == 0
            if not state.lost.active:
                state.lost_all_alerted = False
            elif none_left and not state.lost_all_alerted:
                # the first alert of this loss, or a partial loss (already
                # alerted, as a warning) that has now become total
                state.lost_all_alerted = True
                changed = True
            if changed:
                events.append(
                    {
                        "rule": view.rule("redis_replicas_lost"),
                        "severity": "critical" if none_left else "warning",
                        "detail": (
                            f"Redis{view.tag} has {connected} of "
                            f"{state.expected} replicas connected"
                        ),
                        "message": (
                            f"{view.name('Broker `redis`')} has {connected} "
                            f"connected replica(s), down from {state.expected}. "
                            + (
                                "It has no redundancy left: if it fails now "
                                "there is nothing to fail over to."
                                if none_left
                                else "It has less redundancy than it had."
                            )
                        ),
                    }
                )
            elif changed is False:
                events.append(
                    {
                        "rule": view.rule("redis_replicas_recovered"),
                        "severity": "info",
                        "detail": (
                            f"Redis{view.tag} has all {connected} replicas "
                            "connected again"
                        ),
                        "message": (
                            f"{view.name('Broker `redis`')} has all {connected} "
                            "replica(s) connected again."
                        ),
                    }
                )
            behind = _lagging_replicas(redis)
            changed = state.lagging.update(bool(behind), now, grace)
            if changed:
                events.append(
                    {
                        "rule": view.rule("redis_replica_lagging"),
                        "severity": "warning",
                        "detail": f"Redis{view.tag} replica lagging: "
                        + "; ".join(behind),
                        "message": (
                            f"{view.name('Broker `redis`')} has a replica that "
                            f"is not keeping up: {'; '.join(behind)}. A "
                            "failover to it now would lose what it has not "
                            "received."
                        ),
                    }
                )
            elif changed is False:
                events.append(
                    {
                        "rule": view.rule("redis_replica_caught_up"),
                        "severity": "info",
                        "detail": f"Redis{view.tag} replicas caught up",
                        "message": (
                            f"{view.name('Broker `redis`')} replicas have "
                            "caught up with it again."
                        ),
                    }
                )
            for event in events:
                event = {"ts": now, **event, **view.event_fields()}
                await _record_alert(event)
                await dispatch(event)

    async def handle_redis_eviction_window(self, snapshot: Dict[str, Any]) -> None:
        """Alert when a Redis instance near its cap would evict keys younger
        than a running query (#201).

        The eviction window is ``maxmemory`` divided by the write rate: once
        the instance is full and evicting oldest-first, that is how long a
        newly written key lasts. A full instance with a long window sheds
        finished queries' leftovers, which is what the cap is for; with a
        window shorter than a query's time budget it sheds the payloads of
        queries still running, and they fail. So this fires
        (``redis_eviction_window_short``, critical) only when both hold: at or
        above ``monitor_redis_eviction_window_near_cap_pct`` of maxmemory, and
        a window under ``eviction_window_min_sec()``. Debounced by
        ``monitor_redis_memory_grace_sec`` in both directions, on top of the
        poller's smoothing of the write rate, so one burst doesn't flap it;
        ``redis_eviction_window_recovered`` when it clears. A no-op for an
        uncapped instance.
        """
        now = time.time()
        minimum = eviction_window_min_sec()
        near_cap = settings.monitor_redis_eviction_window_near_cap_pct
        for view in redis_instances.views(snapshot):
            redis = view.stats
            if "error" in redis:
                continue
            window = redis.get("eviction_window_sec")
            pct = redis.get("maxmemory_pct")
            short = (
                window is not None
                and pct is not None
                and pct >= near_cap
                and window < minimum
            )
            held = self._redis_eviction_window.setdefault(view.suffix, _Held())
            changed = held.update(short, now, settings.monitor_redis_memory_grace_sec)
            if changed:
                rate = redis.get("write_bytes_per_sec") or 0
                event = {
                    "rule": view.rule("redis_eviction_window_short"),
                    "severity": "critical",
                    "detail": (
                        f"Redis{view.tag} eviction window {_minutes(window)} "
                        f"(< {_minutes(minimum)}) at {pct:.1f}% of maxmemory"
                    ),
                    "message": (
                        f"{view.name('Redis broker')} is at {pct:.1f}% of its "
                        f"maxmemory and taking {rate / 2**20:.1f} MiB/s of "
                        f"writes, so once full it evicts keys only "
                        f"{_minutes(window)} old -- less than the "
                        f"{_minutes(minimum)} a running query needs its data "
                        "for. Queries in flight will lose their payloads; "
                        "raise maxmemory or shed load."
                    ),
                }
            elif changed is False:
                event = {
                    "rule": view.rule("redis_eviction_window_recovered"),
                    "severity": "info",
                    "detail": f"Redis{view.tag} eviction window long enough again",
                    "message": (
                        f"{view.name('Redis broker')} is no longer at risk of "
                        "evicting the data of running queries."
                    ),
                }
            else:
                continue
            event = {"ts": now, **event, **view.event_fields()}
            await _record_alert(event)
            await dispatch(event)

    async def _in_cooldown(self, name: str) -> bool:
        try:
            return bool(await broker_client.exists(f"alert:cooldown:{name}"))
        except Exception:
            return False

    async def _set_cooldown(self, name: str, cooldown: float) -> None:
        try:
            await broker_client.set(f"alert:cooldown:{name}", "1", ex=int(cooldown))
        except Exception as e:
            logger.debug(f"Failed to set cooldown for {name}: {e}")

    def _buffer_down(self, worker: str, event: Dict[str, Any], now: float) -> None:
        """Queue a worker-down alert for batched delivery.

        Keyed by worker name so a worker that trips more than one rule within
        the same window is only listed once. The debounce window starts when
        the first event lands in an empty buffer.
        """
        if not self._down_buffer:
            self._down_buffer_started_at = now
        self._down_buffer[worker] = event

    async def _maybe_flush_down_buffer(self, now: float) -> None:
        """Send buffered worker-down alerts once the debounce window elapses.

        One worker keeps the original single-worker message; several get
        coalesced into one combined message.
        """
        if not self._down_buffer:
            return
        if (now - self._down_buffer_started_at) < settings.monitor_down_debounce_sec:
            return
        events = list(self._down_buffer.values())
        self._down_buffer.clear()
        self._down_buffer_started_at = 0.0
        if len(events) == 1:
            await dispatch(events[0])
        else:
            await dispatch_batch(events, f"{len(events)} workers down")

    async def evaluate(self, snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return the list of alerts that fired on this snapshot."""
        now = snapshot["ts"]
        fired: List[Dict[str, Any]] = []
        # Refresh the Redis-eviction grace window from this snapshot before we
        # decide whether to trust "worker down" readings below.
        self._note_redis_eviction(snapshot)
        # Worker-down alerts are suppressed at boot (startup grace), around a
        # broker outage (recovery grace), and while Redis is evicting keys
        # (eviction grace): in every case a worker type reads as zero-alive not
        # because it died but because its heartbeat key is missing or hasn't
        # (re-)registered yet. A broker restart or a memory-pressure eviction
        # spell would otherwise turn into ~one worker-crash alert per worker
        # type -- exactly the inaccurate flood these grace windows exist to
        # replace with a single, accurate signal.
        suppress_worker_down = (
            self.in_startup_grace
            or self.in_broker_recovery_grace
            or self.in_redis_eviction_grace
        )
        # A rule is one check, except a per-instance rule (redis_eviction),
        # which is one per Redis instance, each with its own alert name,
        # breach streak and cooldown.
        checks = [
            (rule, *check) for rule in self.rules for check in rule.checks(snapshot)
        ]
        for rule, scope, name, detail, message, extra in checks:
            if detail is None:
                rule.reset_streak(scope)
                continue
            duration_in_breach = now - rule.streak_started_at(now, scope)
            if duration_in_breach < rule.duration:
                continue
            if rule.kind == "heartbeat_lost" and rule.worker and suppress_worker_down:
                # Boot, broker-recovery, or Redis-eviction window: a zero-alive
                # reading here is an artifact of heartbeats not being
                # (re-)registered or their keys having been evicted, not a real
                # loss. Skip WITHOUT arming the cooldown or recording an alert,
                # and leave the breach streak intact -- so a worker that is
                # genuinely still down once the window elapses fires promptly.
                continue
            if await self._in_cooldown(name):
                continue
            await self._set_cooldown(name, rule.cooldown)
            event = {
                "ts": now,
                "rule": name,
                "severity": rule.severity,
                "detail": detail,
                "message": message,
                **extra,
            }
            fired.append(event)
            await _record_alert(event)
            # heartbeat_lost is a worker-down alert: buffer it so it coalesces
            # with any crash/scale-down events for the same flood. All other
            # rule kinds (backlog/threshold) are unrelated and fire immediately.
            if rule.kind == "heartbeat_lost" and rule.worker:
                self._buffer_down(rule.worker, event, now)
            else:
                await dispatch(event)
        # Last-worker-down alerts: critical whenever a worker type hits zero,
        # because every worker type is supposed to have at least one instance
        # running. The message differentiates a crash from a clean scale-down
        # so the operator sees which one happened, but severity is the same.
        for ev in snapshot.get("events", []):
            if not (ev.get("type") == "scale_down" and ev.get("to") == 0):
                continue
            if suppress_worker_down:
                # The whole stack just came up (startup grace), the broker just
                # restarted (recovery grace), or Redis is evicting heartbeat keys
                # under memory pressure (eviction grace): persistent worker state
                # looks "alive" but current heartbeats are missing or haven't
                # arrived yet, so every worker type spuriously reads as crashed.
                # Stay silent until workers have had a chance to (re-)register.
                logger.debug(
                    f"Suppressing worker-down alert for {ev.get('worker')} "
                    "during startup/broker-recovery/redis-eviction grace"
                )
                continue
            kind = ev.get("kind", "unknown")
            if kind == "crashed":
                key = f"worker_crashed:{ev['worker']}"
                detail = (
                    f"{ev['worker']} dropped from {ev['from']} to 0 with no "
                    "shutdown marker"
                )
                message = (
                    f"Worker `{ev['worker']}` appears to have crashed "
                    f"(was {ev['from']}, now 0; no clean-shutdown signal received)."
                )
            else:
                key = f"worker_zero:{ev['worker']}"
                detail = f"{ev['worker']} cleanly scaled down to 0 (was {ev['from']})"
                message = (
                    f"Worker `{ev['worker']}` scaled to zero. Every worker "
                    "type is expected to have at least one instance running."
                )
            if await broker_client.exists(f"alert:cooldown:{key}"):
                continue
            await broker_client.set(f"alert:cooldown:{key}", "1", ex=600)
            event = {
                "ts": now,
                "rule": key,
                "severity": "critical",
                "detail": detail,
                "message": message,
            }
            fired.append(event)
            await _record_alert(event)
            self._buffer_down(ev["worker"], event, now)
        # Deliver any buffered worker-down alerts whose window has elapsed. The
        # poll loop calls evaluate() faster than the debounce window, so this
        # fires on a normal tick without a dedicated timer.
        await self._maybe_flush_down_buffer(now)
        return fired


async def _record_alert(event: Dict[str, Any]) -> None:
    # Recent-alerts list in Redis powers the live dashboard's alerts feed.
    try:
        pipe = broker_client.pipeline()
        pipe.lpush(ALERT_HISTORY_KEY, json.dumps(event))
        pipe.ltrim(ALERT_HISTORY_KEY, 0, ALERT_HISTORY_LIMIT - 1)
        await pipe.execute()
    except Exception as e:
        logger.debug(f"Failed to record alert: {e}")
    # Durable archive in Postgres lets the History tab surface old alerts.
    try:
        from . import storage

        worker = None
        rule = event.get("rule", "")
        # ``worker_crashed:<worker>`` and the like name their worker after the
        # colon. An alert about a Redis instance has colons of its own
        # (``...@shepherd_data:6379``) and no worker.
        if ":" in rule and not event.get("instance"):
            worker = rule.split(":", 1)[1]
        await storage.insert_event(
            event_type="alert",
            worker=worker,
            severity=event.get("severity"),
            detail=event.get("detail"),
            payload=event,
            unix_ts=event.get("ts"),
        )
    except Exception as e:
        logger.debug(f"Failed to archive alert: {e}")


async def recent_alerts(limit: int = 50) -> List[Dict[str, Any]]:
    try:
        raw = await broker_client.lrange(ALERT_HISTORY_KEY, 0, limit - 1)
    except Exception:
        return []
    out = []
    for item in raw:
        try:
            out.append(json.loads(item))
        except json.JSONDecodeError:
            continue
    return out


_SEVERITY_EMOJI = {
    "info": ":information_source:",
    "warning": ":warning:",
    "critical": ":rotating_light:",
}
# Highest-to-lowest so we can pick the most urgent severity across a batch.
_SEVERITY_ORDER = ["critical", "warning", "info"]


def _env_context_line() -> str:
    """Environment context line shared by single and batched Slack messages.

    Lets one Slack channel receive alerts from multiple deployments (dev /
    staging / production) without ambiguity about which one fired.
    """
    server_url = settings.server_url or "unknown"
    maturity = settings.server_maturity or "unknown"
    return f"*Environment:* {maturity}  |  *URL:* <{server_url}|{server_url}>"


def _max_severity(events: List[Dict[str, Any]]) -> str:
    severities = {ev.get("severity", "warning") for ev in events}
    for level in _SEVERITY_ORDER:
        if level in severities:
            return level
    return "warning"


async def dispatch(event: Dict[str, Any]) -> None:
    await asyncio.gather(
        _dispatch_slack(event),
        _dispatch_email(event),
        return_exceptions=True,
    )


async def dispatch_batch(events: List[Dict[str, Any]], summary: str) -> None:
    """Send one combined Slack/email message for several related alerts.

    ``summary`` is the short headline phrase, e.g. ``"18 workers down"`` or
    ``"3 abandoned queries"``.
    """
    await asyncio.gather(
        _dispatch_slack_batch(events, summary),
        _dispatch_email_batch(events, summary),
        return_exceptions=True,
    )


async def _dispatch_slack(event: Dict[str, Any]) -> None:
    url = settings.slack_webhook_url
    if not url:
        return
    emoji = _SEVERITY_EMOJI.get(event.get("severity", "warning"), ":warning:")
    text = (
        f"{emoji} *Shepherd alert* `{event['rule']}` ({event['severity']})\n"
        f"{_env_context_line()}\n"
        f"{event['message']}"
    )
    await _post_slack(url, text)


async def _dispatch_slack_batch(events: List[Dict[str, Any]], summary: str) -> None:
    url = settings.slack_webhook_url
    if not url:
        return
    severity = _max_severity(events)
    emoji = _SEVERITY_EMOJI.get(severity, ":warning:")
    bullets = "\n".join(f"• {ev['message']}" for ev in events)
    text = (
        f"{emoji} *Shepherd alert* — {summary} ({severity})\n"
        f"{_env_context_line()}\n"
        f"{bullets}"
    )
    await _post_slack(url, text)


async def _post_slack(url: str, text: str) -> None:
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(url, json={"text": text})
    except Exception as e:
        logger.warning(f"Slack dispatch failed: {e}")


async def _dispatch_email(event: Dict[str, Any]) -> None:
    if not (settings.alert_email_to and settings.smtp_host):
        return
    await asyncio.get_running_loop().run_in_executor(None, _send_email_sync, event)


async def _dispatch_email_batch(events: List[Dict[str, Any]], summary: str) -> None:
    if not (settings.alert_email_to and settings.smtp_host):
        return
    await asyncio.get_running_loop().run_in_executor(
        None, _send_email_batch_sync, events, summary
    )


def _send_email_sync(event: Dict[str, Any]) -> None:
    msg = EmailMessage()
    msg["Subject"] = f"[Shepherd] {event['severity']}: {event['rule']}"
    msg["From"] = settings.alert_email_from or settings.smtp_user or "shepherd-monitor"
    msg["To"] = settings.alert_email_to
    msg.set_content(
        f"Rule: {event['rule']}\n"
        f"Severity: {event['severity']}\n"
        f"Time: {time.ctime(event['ts'])}\n\n"
        f"{event['message']}\n"
    )
    _smtp_send(msg)


def _send_email_batch_sync(events: List[Dict[str, Any]], summary: str) -> None:
    severity = _max_severity(events)
    msg = EmailMessage()
    msg["Subject"] = f"[Shepherd] {severity}: {summary}"
    msg["From"] = settings.alert_email_from or settings.smtp_user or "shepherd-monitor"
    msg["To"] = settings.alert_email_to
    body = "\n".join(f"- {ev['message']}" for ev in events)
    msg.set_content(
        f"Severity: {severity}\n"
        f"Time: {time.ctime(events[0]['ts'])}\n\n"
        f"{summary}:\n{body}\n"
    )
    _smtp_send(msg)


def _smtp_send(msg: EmailMessage) -> None:
    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as smtp:
            if settings.smtp_use_tls:
                smtp.starttls(context=ssl.create_default_context())
            if settings.smtp_user:
                smtp.login(settings.smtp_user, settings.smtp_password)
            smtp.send_message(msg)
    except Exception as e:
        logger.warning(f"Email dispatch failed: {e}")
