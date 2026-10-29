"""ARS watchdog: the timeout sweep + payload retention.

Port of NCATSTranslator/Relay @ 3e65975 tasks.py catch_timeout_async (celery
beat, every 3 minutes) as a self-scheduling loop. Parity is on the age
thresholds, which match upstream's code exactly: parents are exempt; merge
messages (ars-ars-agent) time out after 8 minutes; everything else after 5
minutes (including pathfinder -- upstream's log line says 10 but its code
compares against now-5min). Timed-out messages get code 598 / status 'E',
and, since 'E' is terminal, the parent completion check runs (upstream got
this via the post_save signal).

Upstream also examined ONLY messages created in the last 15 minutes, which
made a stuck message unreapable forever once it aged out of that window --
a sweep outage longer than (window - threshold) stranded every message it
missed, and their parents stayed Running with no path to completion. The
ceiling is off by default here (ars_timeout_scan_window_sec = 0); the sweep
is bounded by ars_timeout_scan_limit rows per pass instead, oldest first.

The loop also hosts the response-cache repair sweep (shepherd_utils.ars.cache
.repair_sweep): pending entries whose leader is finished or gone, and
superseded cache generations; and storage retention (shepherd_utils.ars
.retention): eviction from the payload volume under disk pressure every
pass, the age-based payload purge and old-tree deletion hourly.

Intent-level deviations, register-documented: upstream resolves the agent by
indexing the Agent table with the ACTOR's pk (a latent bug whose outcome
depends on row-id coincidence); this port reads the agent name recorded on
the message row. It also exempts the ars-workflow-agent parents a volume
from before de-federation may still carry, alongside ars-default-agent
(upstream exempts only the latter, which would 598 workflow parents).
"""

import asyncio
import datetime
import logging
import uuid

import shepherd_utils.ars.cache as cache
import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
import shepherd_utils.ars.retention as retention
from shepherd_utils.config import settings
from shepherd_utils.db import initialize_db
from shepherd_utils.heartbeat import Heartbeat
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer

STREAM = "ars.watchdog"
CONSUMER = str(uuid.uuid4())[:8]
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

PARENT_AGENTS = ("ars-default-agent", "ars-workflow-agent")
MERGE_AGENT = "ars-ars-agent"

_PURGE_INTERVAL_SEC = 3600.0


def _age_seconds(row) -> float:
    now = datetime.datetime.now(datetime.timezone.utc)
    ts = row["ts"]
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=datetime.timezone.utc)
    return (now - ts).total_seconds()


async def sweep(logger: logging.Logger) -> int:
    """One catch_timeout pass. Returns the number of messages timed out."""
    # The floor is the cheapest threshold any row could trip, so the query
    # skips rows that cannot have timed out yet; there is deliberately no
    # ceiling by default (see get_running_messages).
    min_age = min(
        settings.ars_timeout_standard_sec,
        settings.ars_timeout_pathfinder_sec,
        settings.ars_timeout_merge_sec,
    )
    rows = await ars_db.get_running_messages(
        settings.ars_timeout_scan_window_sec,
        min_age_sec=min_age,
        limit=settings.ars_timeout_scan_limit,
    )
    timed_out = 0
    for row in rows:
        agent_name = row.get("agent_name")
        if agent_name in PARENT_AGENTS:
            continue
        age = _age_seconds(row)
        if agent_name == MERGE_AGENT:
            threshold = settings.ars_timeout_merge_sec
        else:
            query_type = (row.get("params") or {}).get("query_type")
            if query_type == "pathfinder":
                threshold = settings.ars_timeout_pathfinder_sec
            else:
                threshold = settings.ars_timeout_standard_sec
        if age > threshold:
            logger.info(
                f"{agent_name} pk: {row['id']} still Running after "
                f"{age:.0f}s (> {threshold:.0f}s), setting its code to 598"
            )
            await ars_db.update_message(row["id"], status="E", code=598)
            timed_out += 1
            if row.get("ref") is not None:
                await lifecycle.check_parent_completion(row["ref"], logger)
    return timed_out


async def retention_pass(logger: logging.Logger, hourly: bool) -> None:
    """Storage retention, each step isolated so one failing doesn't skip the
    rest. Eviction runs every pass so a burst is caught within a minute; the
    age-based passes only hourly."""
    eviction = {}
    try:
        eviction = await retention.evict_for_space(logger)
    except Exception as e:
        logger.error(f"Payload eviction failed: {e}", exc_info=True)
    await retention.publish_usage(eviction, logger)
    if not hourly:
        return
    try:
        purged = await retention.purge_expired_payloads(
            settings.ars_data_retention_days, logger
        )
        if purged:
            logger.info(f"Purged the durable payloads of {purged} old ARS trees")
    except Exception as e:
        logger.error(f"Payload purge failed: {e}", exc_info=True)
    try:
        deleted = await retention.delete_expired_trees(
            settings.ars_row_retention_days, logger
        )
        if deleted:
            logger.info(f"Deleted {deleted} ARS trees past row retention")
    except Exception as e:
        logger.error(f"ARS tree deletion failed: {e}", exc_info=True)


async def run_forever():
    await initialize_db()
    # kept alive for the process lifetime; the loop below never returns
    Heartbeat(STREAM, CONSUMER, 1).start()
    last_purge = 0.0
    loop = asyncio.get_running_loop()
    while True:
        try:
            await sweep(LOGGER)
        except Exception as e:
            LOGGER.error(f"Watchdog sweep failed: {e}", exc_info=True)
        # Response cache upkeep: stale pending leaders, superseded
        # generations (docs/ARS_RESPONSE_CACHE_PLAN.md).
        try:
            repaired = await cache.repair_sweep(LOGGER)
            if any(repaired.values()):
                LOGGER.info(f"Cache repair sweep: {repaired}")
        except Exception as e:
            LOGGER.error(f"Cache repair sweep failed: {e}", exc_info=True)
        now = loop.time()
        hourly = now - last_purge >= _PURGE_INTERVAL_SEC
        if hourly:
            last_purge = now
        await retention_pass(LOGGER, hourly)
        await asyncio.sleep(settings.ars_watchdog_interval_sec)


if __name__ == "__main__":
    asyncio.run(run_forever())
