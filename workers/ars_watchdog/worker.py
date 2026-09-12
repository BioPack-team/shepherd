"""ARS watchdog: the timeout sweep + payload retention.

Port of NCATSTranslator/Relay @ 3e65975 tasks.py catch_timeout_async (celery
beat, every 3 minutes) as a self-scheduling loop. Parity is on the age
thresholds, which match upstream's code exactly: only Running messages
created within the last 15 minutes are examined; parents are exempt; merge
messages (ars-ars-agent) time out after 8 minutes; everything else after 5
minutes (including pathfinder -- upstream's log line says 10 but its code
compares against now-5min). Timed-out messages get code 598 / status 'E',
and, since 'E' is terminal, the parent completion check runs (upstream got
this via the post_save signal).

Intent-level deviations, register-documented: upstream resolves the agent by
indexing the Agent table with the ACTOR's pk (a latent bug whose outcome
depends on row-id coincidence); this port joins actor->agent properly. It
also exempts ars-workflow-agent parents alongside ars-default-agent
(upstream exempts only the latter, which would 598 workflow parents).
"""

import asyncio
import datetime
import logging
import uuid

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
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
    rows = await ars_db.get_running_messages(settings.ars_timeout_scan_window_sec)
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


async def run_forever():
    await initialize_db()
    heartbeat = Heartbeat(STREAM, CONSUMER, 1).start()  # noqa: F841
    last_purge = 0.0
    loop = asyncio.get_running_loop()
    while True:
        try:
            await sweep(LOGGER)
        except Exception as e:
            LOGGER.error(f"Watchdog sweep failed: {e}", exc_info=True)
        now = loop.time()
        if now - last_purge >= _PURGE_INTERVAL_SEC:
            last_purge = now
            try:
                purged = await ars_db.purge_old_message_data(
                    settings.ars_data_retention_days
                )
                if purged:
                    LOGGER.info(f"Purged durable payloads of {purged} old ARS messages")
            except Exception as e:
                LOGGER.error(f"Payload purge failed: {e}")
        await asyncio.sleep(settings.ars_watchdog_interval_sec)


if __name__ == "__main__":
    asyncio.run(run_forever())
