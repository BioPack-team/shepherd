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

    @property
    def in_startup_grace(self) -> bool:
        return (time.time() - self._boot_time) < settings.monitor_startup_grace_sec

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
            await dispatch_batch(events)

    async def evaluate(self, snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return the list of alerts that fired on this snapshot."""
        now = snapshot["ts"]
        fired: List[Dict[str, Any]] = []
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
        startup_grace = self.in_startup_grace
        for ev in snapshot.get("events", []):
            if not (ev.get("type") == "scale_down" and ev.get("to") == 0):
                continue
            if startup_grace:
                # The whole stack just came up; persistent worker state from a
                # previous run looks "alive" but current heartbeats haven't
                # arrived yet. Stay silent until workers have had a chance to
                # register.
                logger.debug(
                    f"Suppressing worker-down alert for {ev.get('worker')} "
                    "during startup grace"
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


async def dispatch_batch(events: List[Dict[str, Any]]) -> None:
    """Send one combined Slack/email message for several worker-down alerts."""
    await asyncio.gather(
        _dispatch_slack_batch(events),
        _dispatch_email_batch(events),
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


async def _dispatch_slack_batch(events: List[Dict[str, Any]]) -> None:
    url = settings.slack_webhook_url
    if not url:
        return
    severity = _max_severity(events)
    emoji = _SEVERITY_EMOJI.get(severity, ":warning:")
    bullets = "\n".join(f"• {ev['message']}" for ev in events)
    text = (
        f"{emoji} *Shepherd alert* — {len(events)} workers down ({severity})\n"
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


async def _dispatch_email_batch(events: List[Dict[str, Any]]) -> None:
    if not (settings.alert_email_to and settings.smtp_host):
        return
    await asyncio.get_running_loop().run_in_executor(
        None, _send_email_batch_sync, events
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


def _send_email_batch_sync(events: List[Dict[str, Any]]) -> None:
    severity = _max_severity(events)
    msg = EmailMessage()
    msg["Subject"] = f"[Shepherd] {severity}: {len(events)} workers down"
    msg["From"] = settings.alert_email_from or settings.smtp_user or "shepherd-monitor"
    msg["To"] = settings.alert_email_to
    body = "\n".join(f"- {ev['message']}" for ev in events)
    msg.set_content(
        f"Severity: {severity}\n"
        f"Time: {time.ctime(events[0]['ts'])}\n\n"
        f"{len(events)} workers went down:\n{body}\n"
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
