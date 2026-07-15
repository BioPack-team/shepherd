"""Alert rule evaluation and dispatch.

Rules are loaded from a YAML file (path configured via ``MONITOR_ALERTS_CONFIG``).
On every snapshot the engine evaluates each rule against the snapshot. A rule
that has been firing continuously for at least ``duration`` seconds emits an
alert. Cooldown state (``alert:cooldown:{rule}``) lives in Redis so we don't
re-notify on every tick.
"""

import asyncio
import json
import logging
import smtplib
import ssl
import time
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import yaml

from shepherd_utils.broker import broker_client
from shepherd_utils.config import settings

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

    def reset_streak(self) -> None:
        self._first_fired_at = None

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
        if self.kind == "redis_memory":
            return self._eval_redis_memory(snapshot)
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
        return None

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

    def _eval_redis_memory(self, snapshot: Dict[str, Any]) -> Optional[str]:
        """Fire when the broker's used memory crosses ``threshold`` percent of
        its ``maxmemory`` cap.

        This is the early warning the OOM-kill incident lacked: eviction (and,
        if the cap sits at the container limit, an OOM-kill) only bites once
        usage nears the cap, so crossing e.g. 85% is the actionable signal. A
        no-op when the broker runs uncapped (``maxmemory`` 0) since there's no
        denominator.
        """
        redis = snapshot.get("redis", {})
        maxmem = redis.get("maxmemory_bytes", 0)
        used = redis.get("used_memory_bytes", 0)
        if not maxmem or self.threshold is None:
            return None
        pct = 100.0 * used / maxmem
        if pct >= float(self.threshold):
            return (
                f"Redis memory {pct:.1f}% of maxmemory "
                f"({used / 1e9:.1f}GB of {maxmem / 1e9:.1f}GB) >= {self.threshold}%"
            )
        return None

    def _eval_redis_eviction(self, snapshot: Dict[str, Any]) -> Optional[str]:
        """Fire when the broker evicted keys in the last interval.

        With the ``volatile-ttl`` policy, eviction only happens once the broker
        is *at* its maxmemory cap, so a non-zero delta is a definitive "the cap
        has been reached and blobs are being shed" signal -- distinct from the
        percentage warning above, which is a lead indicator. ``threshold``
        defaults to 0 (any eviction); raise it to ignore trivial churn.
        """
        redis = snapshot.get("redis", {})
        delta = redis.get("evicted_keys_delta", 0)
        floor = float(self.threshold) if self.threshold is not None else 0.0
        if delta > floor:
            return (
                f"Redis evicted {delta} key(s) in the last interval -- broker is "
                "at its maxmemory cap and shedding data"
            )
        return None

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
        """Open/extend the eviction grace window when this tick saw evictions."""
        redis = snapshot.get("redis", {}) or {}
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

    async def _in_cooldown(self, rule: Rule) -> bool:
        try:
            return bool(await broker_client.exists(f"alert:cooldown:{rule.name}"))
        except Exception:
            return False

    async def _set_cooldown(self, rule: Rule) -> None:
        try:
            await broker_client.set(
                f"alert:cooldown:{rule.name}", "1", ex=int(rule.cooldown)
            )
        except Exception as e:
            logger.debug(f"Failed to set cooldown for {rule.name}: {e}")

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
        for rule in self.rules:
            detail = rule.evaluate(snapshot)
            if detail is None:
                rule.reset_streak()
                continue
            if rule._first_fired_at is None:
                rule._first_fired_at = now
            duration_in_breach = now - rule._first_fired_at
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
            if await self._in_cooldown(rule):
                continue
            await self._set_cooldown(rule)
            event = {
                "ts": now,
                "rule": rule.name,
                "severity": rule.severity,
                "detail": detail,
                "message": rule.message or detail,
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
        if ":" in rule:
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
