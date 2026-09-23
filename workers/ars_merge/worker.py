"""ARS merge worker: fold each validated ARA result into the query's merged
message and post-process the new version.

Port of NCATSTranslator/Relay @ 3e65975 utils.py merge_and_post_process
(lock + bookkeeping), merge_received (the fold), and post_process (the tail
that upstream ran in the same Celery task): one merge at a time per parent,
a fresh merge-child row per fold, parent.merged_version /
merged_versions_list / params.stats advanced, the merged_version_begun and
merged_version_available notifications, and the parent completion check.

Post-processing runs on the merged message right after the fold, in the
same process-pool child that holds it: blocklist removal, node annotation,
local confidence calculation, and score stats, with upstream's exact
stage-failure codes (cleanup stages and the stat calc mark the merged child
'E'/444, and the 444 sticks; appraise_confidence failures are logged and
swallowed; a failed final save is E/422). On success the 202 shell flips to
'D'/200. The external Appraiser call and the Sugeno scoring pass were
removed upstream (Relay PRs #884/#883) -- ordering components come from
appraise_confidence -- and so was the null-attribute scrub (Relay PR #885).
Node annotation runs the biothings_annotator package in-process, as
upstream (parity register R2 pins the package to a specific commit where
Relay installs it unpinned) -- here only with ``ars_annotation_mode``
"merge". The default, "premerge", annotates each response in ars_premerge
instead, so annotation stays off this worker's per-parent lock.

The pool child emits its own spans: the parent injects its span context
into a carrier that rides the pool call, and the child (which sets up its
own tracer provider, see shepherd_utils.otel.setup_pool_child_tracer)
starts ars.merge.fold and ars.postprocess under it, with the annotator's
span and its outbound httpx client spans nested beneath -- the same trace
shape the standalone post-process worker used to produce.

Work arrives through the merge-ready index (shepherd_utils.ars.db
add_ready_child): ars_premerge records each validated child there and wakes
this stream. The worker that wins the parent's lock drains the index in
arrival order, one merged version per child; a worker that loses the lock
simply acks, because the holder will fold its child too. This is the
merge_message worker's pattern, replacing upstream's semaphore-and-retry
loop (which here waited on the lock and re-enqueued itself with no backoff).

Infrastructure substitutions (behavior-preserving): the DB merge_semaphore +
Celery-retry loop becomes the lock-and-drain above (per-parent mutual
exclusion either way; the semaphore column is still maintained for envelope
parity), and the fold + post-process run in a process-pool child fetching
blobs with the sync Redis client so no payload crosses IPC. A fold failure
leaves the shell merge-child Running, exactly like upstream's swallowed
merge_received exception -- the 8-minute watchdog 598s it.
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from opentelemetry.propagate import extract, inject

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
from shepherd_utils.ars import aras
from shepherd_utils.ars.annotate import annotate_nodes
from shepherd_utils.ars.blocklist import load_blocklist, remove_blocked
from shepherd_utils.ars.merge import (
    TranslatorMessage,
    get_msg_stats,
    mergeMessages,
)
from shepherd_utils.ars.notify import notify_subscribers
from shepherd_utils.ars.premerge import (
    ScoreStatCalc,
    add_log_entry,
    appraise_confidence,
    get_safe,
    timestamp_hms,
)
from shepherd_utils.broker import (
    add_task,
    mark_task_as_complete,
    refresh_lock,
    remove_lock,
    try_lock,
)
from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.db import get_message_sync, save_logs, save_message_sync
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_pool_child_tracer, setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import get_tasks

STREAM = "ars.merge"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

_pool = None
_loop = None


# ---------------------------------------------------------------------------
# pool side: fold
# ---------------------------------------------------------------------------


def _fold(current_pk, child_pk) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """merge_received's data path: the incoming child blob's ``message``
    part is the newcomer; when a merged version already exists its
    ``message`` part is folded in via mergeMessages, otherwise the newcomer
    becomes the first merged version. Returns (merged dict, stats)."""
    to_merge = get_message_sync(str(child_pk))["message"]
    t_to_merge = TranslatorMessage(to_merge)
    if current_pk is not None:
        current = get_message_sync(str(current_pk))["message"]
        merged = mergeMessages([TranslatorMessage(current), t_to_merge], "")
    else:
        merged = t_to_merge
    merged_dict = merged.to_dict()
    return merged_dict, get_msg_stats(merged_dict)


def merge_in_child(current_pk, child_pk, new_pk):
    """Pool-side fold only: fetch by id, merge, save once; only stats cross
    IPC. (merge_and_postprocess_in_child is what production runs; this is
    the fold on its own, kept for the golden-backed fold test.)"""
    merged_dict, stats = _fold(current_pk, child_pk)
    save_message_sync(str(new_pk), merged_dict)
    return stats


# ---------------------------------------------------------------------------
# pool side: post-process (upstream utils.post_process)
# ---------------------------------------------------------------------------


def _post_processing_error(merged_row, data, text):
    """utils.post_processing_error's visible effect: the extra log entry
    stamped with the row's updated_at (its in-memory E/206 is always
    overwritten by the calling handler before anything is saved)."""
    updated_at = merged_row.get("updated_at")
    if isinstance(updated_at, datetime):
        stamp = updated_at.strftime("%H:%M:%S")
    elif isinstance(updated_at, str) and updated_at:
        try:
            stamp = datetime.fromisoformat(updated_at).strftime("%H:%M:%S")
        except ValueError:
            stamp = timestamp_hms()
    else:
        stamp = timestamp_hms()
    add_log_entry(data, [text, stamp, "DEBUG"])


async def postprocess_message(
    data, merged_row, agent_name, logger, annotate: bool = True
) -> Dict[str, Any]:
    """Run upstream's post_process stages over a merged message in place.

    Pure with respect to the database: the caller persists ``data`` and
    applies the returned outcome ({status, code, result_count,
    result_stat}) to the merged row. Upstream wrote each stage failure to
    the row as it happened and then overwrote it with the final state; the
    final state is the only thing a reader could observe, so it is all
    that is produced here. ``merged_row`` supplies the row's code (202 for
    a fresh shell) and updated_at (for the error log stamps). ``annotate``
    False skips node annotation (``ars_annotation_mode`` "premerge", where
    each response was annotated before it reached the merge, or "off").
    """
    # local code/status mirror upstream's sticky variables
    code = None
    status = None
    row_code = merged_row.get("code", 202)

    # 1. blocklist
    try:
        remove_blocked(data, load_blocklist(), str(merged_row.get("id")))
    except Exception as e:
        status = "E"
        code = 444
        logger.exception(
            f"Problem with block list removal for agent: {agent_name} pk: "
            f"{merged_row.get('id')}: {e}"
        )
        row_code = 444

    # 2. annotate (the null-attribute scrub that sat here was removed
    # upstream, Relay PR #885); skipped when annotation happens in
    # premerge or is switched off
    if annotate:
        try:
            await annotate_nodes(data, agent_name, logger)
            logger.info(
                f"node annotation successful for agent {agent_name} and pk: "
                f"{merged_row.get('id')}"
            )
        except Exception as e:
            status = "E"
            code = 444
            add_log_entry(
                data,
                [
                    f"node annotation internal error: {str(e)}",
                    timestamp_hms(),
                    "DEBUG",
                ],
            )
            logger.exception(
                f"problem with node annotation for agent: {agent_name} pk: "
                f"{merged_row.get('id')}"
            )
            row_code = 444

    # 3. confidence + stats (only when there are results)
    result_count = None
    result_stat = None
    stat_calc_failed = False
    results = get_safe(data, "message", "results")
    if results is not None and len(results) > 0:
        logger.info(
            f"calculating Confidence for agent {agent_name} and pk "
            f"{merged_row.get('id')}"
        )
        try:
            appraise_confidence(results)
        except Exception:
            # upstream only logs; the pipeline keeps going
            logger.exception(
                f"confidence calculations failed mesg for agent {agent_name} "
                f"is {row_code}"
            )
        try:
            result_count = len(results)
            result_stat = ScoreStatCalc(results)
            logger.info(
                f"scoring stat calculation succeeded for agent {agent_name} "
                f"and pk {merged_row.get('id')}"
            )
        except Exception:
            logger.exception("Error in ScoreStatCalculation or result count")
            _post_processing_error(merged_row, data, "Error in score stat calculation")
            add_log_entry(
                data,
                ["Error in score stat calculation", timestamp_hms(), "DEBUG"],
            )
            status = "E"
            code = 444
            stat_calc_failed = True

    # 4. the 202 shell flips to D/200 only when nothing failed and the final
    # save (the caller's) succeeds; a stat-calc failure returned early
    # upstream, before that flip
    if not stat_calc_failed and row_code == 202:
        code = 200
        status = "D"

    return {
        "status": status or "E",
        "code": code or row_code,
        "result_count": result_count,
        "result_stat": result_stat,
    }


class _CaptureHandler(logging.Handler):
    """Collects (level, message) pairs so a pool child can hand its stage
    logs back for the parent's query logger to record."""

    def __init__(self):
        super().__init__()
        self.lines: List[Tuple[int, str]] = []

    def emit(self, record):
        message = record.getMessage()
        if record.exc_info:
            message = f"{message}: {record.exc_info[1]!r}"
        self.lines.append((record.levelno, message))


def merge_and_postprocess_in_child(
    current_pk,
    child_pk,
    new_pk,
    agent_name,
    row_updated_at,
    otel_carrier=None,
    annotate=True,
) -> Dict[str, Any]:
    """Pool-side fold + post-process. Fetches the blobs by pk, saves the
    fold (so a post-process crash still leaves a merged payload, as
    upstream's fold-time save did), post-processes the dict it already
    holds, and saves the final version. Only the stats, the outcome, and
    the stage log lines cross IPC.

    ``otel_carrier`` is the parent's span context (W3C traceparent); the
    fold and post-process spans start under it so the child's work shows up
    in the query's trace. ``annotate`` is postprocess_message's.
    """
    setup_pool_child_tracer(STREAM)
    parent_ctx = extract(otel_carrier) if otel_carrier else None

    with tracer.start_as_current_span("ars.merge.fold", context=parent_ctx) as span:
        span.set_attribute("agent", agent_name)
        span.set_attribute("merge.child_pk", str(child_pk))
        span.set_attribute("merge.new_pk", str(new_pk))
        span.set_attribute("merge.first", current_pk is None)
        merged_dict, stats = _fold(current_pk, child_pk)
        save_message_sync(str(new_pk), merged_dict)
        for key in ("results", "knowledge_graph_nodes", "knowledge_graph_edges"):
            if key in stats:
                span.set_attribute(f"merge.{key}", stats[key])

    capture = _CaptureHandler()
    child_logger = logging.getLogger(f"shepherd.ars.merge.child.{new_pk}")
    child_logger.setLevel(logging.DEBUG)
    child_logger.propagate = False
    child_logger.addHandler(capture)
    try:
        merged_row = {"id": str(new_pk), "code": 202, "updated_at": row_updated_at}
        with tracer.start_as_current_span(
            "ars.postprocess", context=parent_ctx
        ) as span:
            span.set_attribute("agent", agent_name)
            span.set_attribute("merged.pk", str(new_pk))
            # A fresh loop: this runs in a spawned pool child (or, in tests,
            # a worker thread), where no loop is running -- the equivalent
            # of upstream's run_until_complete around the annotator call.
            # The loop's tasks inherit this span as their current context.
            outcome = asyncio.run(
                postprocess_message(
                    merged_dict, merged_row, agent_name, child_logger, annotate
                )
            )
            try:
                save_message_sync(str(new_pk), merged_dict)
            except Exception:
                # upstream's DatabaseError on the final save -> E/422
                child_logger.exception("Final save failed")
                outcome["status"] = "E"
                outcome["code"] = 422
            span.set_attribute("postprocess.status", outcome["status"])
            span.set_attribute("postprocess.code", outcome["code"])
            if outcome.get("result_count") is not None:
                span.set_attribute("postprocess.result_count", outcome["result_count"])
    finally:
        child_logger.removeHandler(capture)
    outcome["stats"] = stats
    outcome["logs"] = capture.lines
    return outcome


def _warm_pool_child():
    """Pool prewarm hook: the spawn already imported this module; set up the
    child's tracer too, so a real merge pays for neither."""
    setup_pool_child_tracer(STREAM)


async def _run_merge_in_pool(
    current_pk, child_pk, new_pk, agent_name, row_updated_at, otel_carrier, logger
) -> Dict[str, Any]:
    """Indirection for tests; production runs the fold + post-process in
    the pool. The merge annotates only in ``ars_annotation_mode`` "merge"
    (upstream's placement); "premerge" annotated each response already."""
    annotate = settings.ars_annotation_mode == "merge"
    args = (
        current_pk,
        child_pk,
        new_pk,
        agent_name,
        row_updated_at,
        otel_carrier,
        annotate,
    )
    if _pool is not None and _loop is not None:
        return await _pool.run(_loop, merge_and_postprocess_in_child, *args)
    return await asyncio.to_thread(merge_and_postprocess_in_child, *args)


# ---------------------------------------------------------------------------
# worker side: lock, drain, bookkeeping
# ---------------------------------------------------------------------------


def _lock_key(parent_pk) -> str:
    return f"ars-merge:{parent_pk}"


# ``try_lock`` sets a 45s TTL. A fold of two multi-MB messages routinely
# runs longer than that, and a lapsed lock lets a second ars_merge task start
# folding the same parent: both read the same ``merged_version``, both write a
# new one, and whichever UPDATE lands last silently drops the other ARA's
# merge. Keep the lock alive for as long as we actually hold it.
LOCK_TTL_MS = 45_000
LOCK_REFRESH_SEC = 15.0


async def _keep_lock_alive(key: str, logger: logging.Logger):
    """Refresh our own lock every LOCK_REFRESH_SEC until cancelled."""
    while True:
        await asyncio.sleep(LOCK_REFRESH_SEC)
        try:
            if not await refresh_lock(key, CONSUMER, LOCK_TTL_MS, logger):
                # someone else holds it now -- nothing we can do but say so
                logger.warning(f"Lost the merge lock {key} before finishing")
                return
        except Exception as e:  # never let the keep-alive kill the merge
            logger.debug(f"Lock refresh for {key} failed: {e}")


def _replay_child_logs(lines, logger: logging.Logger) -> None:
    for level, message in lines or []:
        logger.log(level, message)


async def merge_one(parent_pk, child_pk, logger: logging.Logger) -> None:
    """Fold one validated child into the parent's merged message and
    post-process the new version: merge_and_post_process for one result."""
    parent = await ars_db.get_message_row(parent_pk)
    if parent is None:
        logger.error(f"Merge: parent {parent_pk} does not exist; skipping")
        return
    child = await ars_db.get_message_row(child_pk)
    if child is None:
        logger.error(f"Merge: child {child_pk} does not exist; skipping")
        return
    agent_name = str(child.get("agent"))

    await ars_db.update_message(parent_pk, merge_semaphore=True)
    merged_shell = await ars_db.create_message(
        agent=aras.MERGE_AGENT, status="Running", code=202, ref=parent_pk
    )
    new_pk = merged_shell["id"]
    current_pk = parent.get("merged_version")
    logger.info(f"Beginning merge for agent {agent_name} with current_pk: {current_pk}")
    updated_at = merged_shell.get("updated_at")
    # the pool child continues this trace: its fold / post-process spans
    # (and the annotator's httpx calls) nest under the current task span
    carrier: Dict[str, str] = {}
    inject(carrier)
    try:
        outcome = await _run_merge_in_pool(
            str(current_pk) if current_pk else None,
            str(child_pk),
            str(new_pk),
            agent_name,
            updated_at.isoformat() if isinstance(updated_at, datetime) else None,
            carrier,
            logger,
        )
    except Exception as e:
        # merge_received swallows and returns {} -- the shell merge child
        # stays Running for the watchdog to reap
        logger.error(f"problem with merging for {agent_name}: {e}", exc_info=True)
        await ars_db.update_message(parent_pk, merge_semaphore=False)
        return
    _replay_child_logs(outcome.get("logs"), logger)
    stats = outcome.get("stats") or {}

    # merge_and_post_process's bookkeeping on the parent
    params = parent.get("params") or {}
    params["stats"] = stats
    mvl = parent.get("merged_versions_list")
    pk_infores_merge = [str(new_pk), agent_name]
    if mvl is None:
        mvl = [pk_infores_merge]
    else:
        mvl = list(mvl) + [pk_infores_merge]
    updated_parent = await ars_db.update_message(
        parent_pk,
        merged_version=str(new_pk),
        merged_versions_list=mvl,
        params=params,
        merge_semaphore=False,
    )
    await notify_subscribers(
        updated_parent if updated_parent else parent,
        {
            "event_type": "merged_version_begun",
            "complete": False,
            "merged_versions_list": mvl if mvl is not None else [],
        },
        logger,
    )

    # post_process's final state on the merged child
    final_updates: Dict[str, Any] = {
        "status": outcome.get("status") or "E",
        "code": outcome.get("code") or 202,
    }
    result_count = outcome.get("result_count")
    if result_count is not None:
        final_updates["result_count"] = result_count
    if outcome.get("result_stat") is not None:
        final_updates["result_stat"] = outcome["result_stat"]
    await ars_db.update_message(new_pk, skip_coercion=False, **final_updates)
    await ars_db.persist_data_copy(new_pk, logger)

    if result_count is not None:
        # Carry the merge's result count up to the parent. Nothing used to
        # set it, which left every stats-bearing notification unsent (the
        # stats block in build_notification keys off the parent's
        # result_count) and the parent's own envelope reporting null results
        # for a query that had them.
        await ars_db.update_message(parent_pk, result_count=result_count)

    parent = await ars_db.get_message_row(parent_pk)
    if parent is not None:
        await notify_subscribers(
            parent,
            {
                "event_type": "merged_version_available",
                "complete": False,
                "merged_version": str(new_pk),
                "merged_versions_list": (
                    parent.get("merged_versions_list")
                    if parent.get("merged_versions_list") is not None
                    else []
                ),
                "stats": stats,
            },
            logger,
        )
    logger.info(f"merged and post-processed {new_pk} for agent {agent_name}")
    await lifecycle.check_parent_completion(parent_pk, logger)


async def ars_merge(task, logger: logging.Logger):
    parent_pk = task[1]["parent_pk"]
    key = _lock_key(parent_pk)
    # Non-blocking: the worker that holds the lock drains the whole parent,
    # so a loser has nothing useful to add -- its child is in the ready index
    # and the holder will fold it.
    if not await try_lock(key, CONSUMER, logger, ttl_sec=LOCK_TTL_MS // 1000):
        logger.debug(f"Merge lock busy for {parent_pk}; the holder will drain it")
        return

    keepalive = asyncio.create_task(_keep_lock_alive(key, logger))
    folded = 0
    try:
        # Drain the parent to empty, one merged version per child, oldest
        # first. Re-reading the index each pass sweeps up children that
        # validated while we were folding.
        while True:
            ready = await ars_db.get_ready_children(parent_pk, logger)
            if not ready:
                break
            for child_pk in ready:
                try:
                    with tracer.start_as_current_span("ars.merge.one") as span:
                        span.set_attribute("merge.parent_pk", str(parent_pk))
                        span.set_attribute("merge.child_pk", str(child_pk))
                        await merge_one(parent_pk, child_pk, logger)
                    folded += 1
                except Exception as e:
                    logger.error(
                        f"Merge of {child_pk} into {parent_pk} failed: {e}",
                        exc_info=True,
                    )
                finally:
                    # attempted once, whatever happened: a child the merge
                    # cannot take must not wedge the drain
                    await ars_db.clear_ready_child(parent_pk, child_pk, logger)
    finally:
        keepalive.cancel()
        try:
            await keepalive
        except asyncio.CancelledError:
            pass
        await remove_lock(key, CONSUMER, logger)

    logger.info(f"Merged {folded} result(s) into {parent_pk}")
    # Close the race where a child landed in the index after our final read
    # but before the lock was released: its own wake task found the lock held
    # and acked. Now that the lock is free, kick exactly one wake if anything
    # remains so the late arrival still gets merged.
    leftover = await ars_db.get_ready_children(parent_pk, logger)
    if leftover:
        logger.debug(
            f"{len(leftover)} child(ren) of {parent_pk} arrived post-drain; "
            "kicking one wake task"
        )
        await add_task(
            STREAM,
            {k: v for k, v in task[1].items() if k != "_started_at"},
            logger,
        )


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    with tracer.start_as_current_span(STREAM, context=parent_ctx):
        try:
            await ars_merge(task, logger)
        except Exception as e:
            logger.error(f"Task {task[0]} failed: {e}", exc_info=True)
        finally:
            try:
                await mark_task_as_complete(STREAM, GROUP, task[0], logger)
            except Exception as e:
                logger.error(f"Task {task[0]}: failed to ack: {e}")
            await save_logs(task[1].get("parent_pk", "ars"), logger)
            limiter.release()


async def poll_for_tasks():
    global _pool, _loop
    _loop = asyncio.get_running_loop()
    max_workers = resolve_pool_workers(TASK_LIMIT, LOGGER)
    LOGGER.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    # One pool call folds AND post-processes (annotation included), so it
    # gets the watchdog's merge budget rather than the generic pool timeout:
    # a merged version still Running past ars_timeout_merge_sec is 598'd
    # anyway, and a shorter cutoff would kill legitimate long annotations.
    _pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="ars_merge process pool",
        task_timeout=max(
            settings.pool_task_timeout_sec, settings.ars_timeout_merge_sec
        ),
        warmup=_warm_pool_child if settings.pool_prewarm else None,
    )
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, max_workers
            ):
                asyncio.create_task(process_task(task, parent_ctx, logger, limiter))
        except asyncio.CancelledError:
            LOGGER.info("Poll loop cancelled, shutting down.")
            _pool.shutdown()
            return
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
