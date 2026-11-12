"""ARS pre-merge worker.

Receives an ARA's response off the broker and runs the per-response pipeline
that upstream executes inline in its Django result-callback view: the intake
state machine (guards, counts, the ara_response_complete notification), then
the premerge stages, then the hand-off to the merge. TRAPI 2.0 throughout,
validated against translator_tom's models (shepherd_utils.ars.trapi).
Moved off the server because this is the CPU-heavy stretch of the callback
path and it saturated the server under concurrent load
(documented deviation in the parity register; the outcome contract below is
upstream's, applied asynchronously).

The premerge stages, in order (``_premerge_stages``):
  - explicit nulls are stripped (TRAPI 2.0 has none) and, when there were
    any, reported as a WARNING on the query -- the first step of removing
    the stage (see ``_strip_and_report_nulls``)
  - edges whose sources do not name the ARA's infores are reported as a
    WARNING; upstream stamped the infores onto them, the port leaves an
    ARA's provenance as sent
  - scores are normalized (pre_merge_process)
  - when validating: support_graphs attributes naming an auxiliary graph the
    response does not contain are removed, and reported as an ERROR, then
    the response is validated
  - with ``ars_annotation_mode = "premerge"``, a validated ``ara-``
    response's nodes are annotated (shepherd_utils.ars.annotate), in
    parallel per response rather than under the parent's merge lock
    (ars_merge then skips the stage). A failed annotation is logged on the
    response and its nodes are left unannotated -- the response still merges.
See docs/ARS_PARITY_REGISTER.md, deviations 14, 18, 19 and 21.

Outcomes, exactly as upstream's view produced them:
  - success           -> premerged payload saved under the child, the child
                         recorded as merge-ready and ars.merge woken (ara-
                         agents), child flips to D/200 (or the status carried
                         on the task), parent completion check
  - invalid TRAPI     -> child E/422 + ara_failed_validation notification
                         (upstream also answered HTTP 422 inline; that
                         response is now a 201, see the register)
  - pipeline crash    -> child E/500 with the "Internal ARS Server Error"
                         log entry, upstream's generic handler behavior
  - ARA query failed  -> child E/<code>: the code the ARA's worker named
                         (501 for a query it doesn't implement), else 500 --
                         what upstream records for an ARA answering non-200

finish_query enqueues {intake_child_pk, response_id} when an ARA pipeline
finishes an ARS-originated query (the de-federated ARS receives every
response over the broker; there is no callback endpoint), and
intake_internal_response runs the upstream callback view's intake state
machine here -- same guards, counts, notifications, and terminal shapes.

The work on the response itself is pure-Python CPU over a multi-MB dict, so
it runs in a process pool, and in ONE pool call per delivery
(``intake_in_child``): the pool child loads the ARA's response by id once,
counts its results and computes the score statistics, runs the premerge
stages when the response is headed for the merge, and saves it once under
the child's pk. Only the counts, the verdict and the stage reports cross
IPC (the same pattern as ars_merge and merge_message), and nothing large is
ever resident in this process. A delivery that ends at intake (no results,
a failed ARA query) still has its response saved under the child, as the
ARA sent it. A task that already names a ``child_pk`` (no intake) premerges
the child's stored payload in place (``premerge_in_child``).

The stage reports are logged in the pool child through a
``pool_task_logger`` and folded into the task's query logs here
(``ingest_query_logs``), so they reach the ARS query's stored logs.

The child's payload carries no query logs: they are in the query's log list
already, and the merge folds only each child's ``message``. Once a tree's
Redis copies expire, a child is served from its ARA's archived response,
which does carry them (ars_db._load_archived).
"""

import asyncio
import datetime
import logging
import uuid
from typing import Any, Dict

from opentelemetry.propagate import extract, inject

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
from shepherd_utils.ars import aras
from shepherd_utils.ars.annotate import annotate_nodes
from shepherd_utils.ars.notify import notify_subscribers
from shepherd_utils.ars.premerge import (
    ScoreStatCalc,
    add_log_entry,
    get_safe,
    log_timestamp,
    pre_merge_process,
    remove_phantom_support_graphs,
)
from shepherd_utils.ars.statuses import coerce_status
from shepherd_utils.ars.trapi import count_stripped_nulls, validate
from shepherd_utils.broker import add_task, mark_task_as_complete
from shepherd_utils.config import settings
from shepherd_utils.query_errors import (
    DEFAULT_ERROR_CODE,
    is_error_status,
    status_error_code,
)
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.db import (
    get_message_sync,
    save_logs,
    save_message_sync,
)
from shepherd_utils.logger import (
    get_worker_logger,
    ingest_query_logs,
    pool_task_logger,
    transient_logger,
)
from shepherd_utils.otel import setup_pool_child_tracer, setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import run_poll_loop

STREAM = "ars.premerge"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

_pool = None
_loop = None


def _warm_pool_child():
    """Pool prewarm hook: the spawn already imported this module; set up the
    child's tracer too, so a real task pays for neither."""
    setup_pool_child_tracer(STREAM)


def _annotate_in_child(data, agent_name, child_pk) -> None:
    """Annotate a validated response's nodes in place. A failure never fails
    the response: it is logged (and noted in the message's logs, as the
    merge's annotation stage does) and the response merges unannotated."""
    # Unregistered, so this long-lived pool child doesn't keep one per response.
    logger = transient_logger(
        f"shepherd.ars.premerge.child.{child_pk}", logging.getLogger("shepherd")
    )
    try:
        asyncio.run(annotate_nodes(data, agent_name, logger))
    except Exception as e:
        logger.exception(f"node annotation failed for {agent_name} pk {child_pk}")
        add_log_entry(
            data,
            [
                f"node annotation internal error: {str(e)}",
                log_timestamp(),
                "DEBUG",
            ],
        )


#: How many example ids a premerge report names; the count is always exact.
REPORT_EXAMPLES = 5


def _examples(ids) -> str:
    ids = list(ids)
    shown = ", ".join(str(i) for i in ids[:REPORT_EXAMPLES])
    return shown + (", ..." if len(ids) > REPORT_EXAMPLES else "")


def _strip_and_report_nulls(data, child_pk, agent_name, inforesid, report) -> None:
    """Strip the response's explicit nulls, reporting them if there were any.

    Step one of removing this stage. TRAPI 2.0 forbids nulls, so anything
    reported here is a bug in the ARA, to be fixed there. Shepherd's own
    response pruning (shepherd_utils.trapi.prune_response) is targeted
    rather than recursive, so a null deep in a response can survive it and
    reach the ARS; this recursive strip is what still keeps such a null out
    of the merge (and makes a null on an optional member read as absent for
    validation). Once these warnings stay quiet, the stage can go.
    """
    removed = count_stripped_nulls(data)
    if removed:
        report.warning(
            f"ARA {agent_name} ({inforesid}) sent {removed} explicit null "
            f"member(s) in child {child_pk}; TRAPI 2.0 forbids nulls, so they "
            "were stripped. This is a bug to fix in the ARA."
        )


def _premerge_stages(
    data, child_pk, agent_name, inforesid, do_validate, annotate, report
) -> bool:
    """Run the premerge stages over ``data`` in place and return the
    validation verdict (True when not validating).

    ``report`` is the logger the per-response reports go to; in the pool
    child it is a pool_task_logger, whose records the worker folds into the
    query's logs. Raises on a stage failure.
    """
    _strip_and_report_nulls(data, child_pk, agent_name, inforesid, report)
    missing_infores = pre_merge_process(data, str(child_pk), agent_name, inforesid)
    if missing_infores:
        # Upstream stamped the ARA's infores onto these edges. A hosted ARA
        # is expected to name itself on every edge it returns, so the port
        # leaves the provenance as sent and says so.
        report.warning(
            f"ARA {agent_name} ({inforesid}): {len(missing_infores)} knowledge "
            f"graph edge(s) in child {child_pk} do not name {inforesid} in "
            f"their sources (e.g. {_examples(missing_infores)}); left as sent"
        )
    if do_validate:
        phantoms = remove_phantom_support_graphs(data)
        if phantoms:
            # Removed so the merge (which joins edges to auxiliary graphs)
            # still works, but a dangling support graph means the ARA lost
            # part of its own answer: loud, so it gets fixed at the source.
            missing_aux = list(
                dict.fromkeys(aux for ids in phantoms.values() for aux in ids)
            )
            report.error(
                f"ARA {agent_name} ({inforesid}): {len(phantoms)} edge(s) in "
                f"child {child_pk} name support graphs the response does not "
                f"contain (edges e.g. {_examples(phantoms)}; missing auxiliary "
                f"graphs e.g. {_examples(missing_aux)}); those support_graphs "
                "attributes were removed"
            )
        valid = validate(data)
    else:
        valid = True
    if annotate and valid:
        _annotate_in_child(data, agent_name, child_pk)
    return bool(valid)


def premerge_in_child(
    child_pk,
    agent_name,
    inforesid,
    do_validate,
    otel_carrier=None,
    annotate=False,
    log_level=logging.INFO,
) -> Dict[str, Any]:
    """Pool-side premerge of a payload already stored under the child (a
    task that names a ``child_pk``; a delivery goes through
    ``intake_in_child``): fetch it by pk, process it in place, write it back.

    Returns ``{"valid": verdict, "logs": [...]}``, the logs being the stage
    reports for the worker to fold into the query's logs. The processed
    payload is saved whether or not it validates, as upstream saved the
    (already premerged) data on both branches of its view. ``annotate``
    adds node annotations to a response that validated. ``otel_carrier`` is
    the parent's span context; the stage span starts under it (see
    shepherd_utils.otel.setup_pool_child_tracer).
    """
    setup_pool_child_tracer(STREAM)
    parent_ctx = extract(otel_carrier) if otel_carrier else None
    with pool_task_logger(STREAM, log_level) as (report, handler):
        with tracer.start_as_current_span(
            "ars.premerge.process", context=parent_ctx
        ) as span:
            span.set_attribute("agent", agent_name)
            span.set_attribute("premerge.child_pk", str(child_pk))
            span.set_attribute("premerge.validate", bool(do_validate))
            data = get_message_sync(str(child_pk))
            valid = _premerge_stages(
                data, child_pk, agent_name, inforesid, do_validate, annotate, report
            )
            span.set_attribute("premerge.annotate", bool(annotate and valid))
            save_message_sync(str(child_pk), data)
            span.set_attribute("premerge.valid", valid)
        logs = handler.drain()
    return {"valid": valid, "logs": logs}


def _annotate_for(agent_name) -> bool:
    """Only responses headed for the merge (ara- agents) are annotated here."""
    return settings.ars_annotation_mode == "premerge" and str(agent_name).startswith(
        "ara-"
    )


def _do_validate(mesg) -> bool:
    """upstream's ``params.validate`` switch: validate unless it is falsy."""
    params = mesg.get("params") or {}
    return not ("validate" in params.keys() and not params["validate"])


async def _run_premerge_in_pool(
    child_pk, agent_name, inforesid, do_validate, otel_carrier, logger
):
    """Indirection for tests; production runs premerge_in_child in the pool."""
    args = (
        child_pk,
        agent_name,
        inforesid,
        do_validate,
        otel_carrier,
        _annotate_for(agent_name),
        logger.getEffectiveLevel(),
    )
    if _pool is not None and _loop is not None:
        return await _pool.run(_loop, premerge_in_child, *args)
    return await asyncio.to_thread(premerge_in_child, *args)


def _row_timestamp(updated_at) -> str:
    """The row's updated_at as an RFC 3339 log timestamp (TRAPI 2.0).

    Upstream wrote ``str(updated_at)`` -- a space-separated datetime, or
    ``"None"`` for a row without one -- neither a valid 2.0 LogEntry
    timestamp. Falls back to now."""
    if isinstance(updated_at, datetime.datetime):
        return log_timestamp(updated_at)
    if isinstance(updated_at, str) and updated_at:
        try:
            return log_timestamp(datetime.datetime.fromisoformat(updated_at))
        except ValueError:
            pass
    return log_timestamp()


async def _terminal_error(child_pk, parent_pk, mesg, data, logger, have_response=True):
    """Upstream's generic callback handler: E/500 + the log entry.

    ``data`` may be None when the failure happened in the pool child (the
    payload was never resident here); the stored blob is loaded to carry
    the log entry, as upstream appended it to the payload it was holding.
    ``have_response`` is False when the ARA's response never reached the
    ARS, so the child has no archived response to be read from later and
    keeps a durable copy of its own (``ars_db.persist_child_copy``).
    """
    if data is None:
        data = await ars_db.load_message_data(child_pk, logger)
    if not isinstance(data, dict):
        data = {}
    log_entry = {
        "message": "Internal ARS Server Error",
        "timestamp": _row_timestamp(mesg.get("updated_at")),
        "level": "ERROR",
    }
    if isinstance(data.get("logs"), list):
        data["logs"].append(log_entry)
    else:
        data["logs"] = [log_entry]
    await ars_db.save_message_data(child_pk, data, logger)
    await ars_db.update_message(child_pk, status="E", code=500)
    await ars_db.persist_child_copy(mesg, logger, have_response=have_response)
    await lifecycle.check_parent_completion(parent_pk, logger)


async def intake_internal_response(fields, logger: logging.Logger):
    """Receive an ARA's response off the queue.

    finish_query enqueues {intake_child_pk, response_id}; this runs the
    upstream callback view's state machine over the stored response (same
    guards, counts, notification, and terminal shapes, minus the HTTP
    answers nobody read).

    On the result-bearing path the response is premerged and saved under
    the child pk in the same pool call; this fills the standard premerge
    task fields into ``fields`` and returns ``{"valid": verdict}`` so
    ``ars_premerge`` hands the child on (or fails it) in this same task.
    Every other outcome is handled here and returns None.
    """
    child_pk = fields["intake_child_pk"]
    response_id = fields["response_id"]
    mesg = await ars_db.get_message_row(child_pk)
    if mesg is None:
        logger.error(f"Intake: child {child_pk} not found")
        return None
    if mesg.get("ref"):
        # so the finally-block log flush (and with it the premerge stage
        # reports) lands under the query's parent
        fields["parent_pk"] = str(mesg["ref"])
    # Upstream's callback received the payload with the query's logs spliced
    # in. The child's payload carries none: they are in the query's log list
    # already, and the merge folds only each child's message.

    # How the ARA query ended (finish_query passes it along). One whose
    # operation failed is what an HTTP ARA answering non-200 is to
    # upstream: the child errors with that code -- 501 when the ARA's
    # worker said it doesn't do this kind of query (BTE on Pathfinder),
    # 500 for a plain failure -- instead of reading as a clean Done with
    # no results. Deliveries from before this field existed carry none.
    ara_status = fields.get("ara_status")
    failed_code = None
    if is_error_status(ara_status):
        failed_code = status_error_code(ara_status) or DEFAULT_ERROR_CODE
    # broker deliveries carry no tr_ars.message.status
    status = "E" if failed_code is not None else "D"
    agent_name = str(mesg.get("agent"))
    inforesid = aras.inforesid_for(agent_name)
    parent = await ars_db.get_message_row(mesg["ref"]) if mesg.get("ref") else None

    # The callback endpoint's guard order: dup-Done, repeated results, errored
    # child. Decided from the rows alone, before the payload is touched, so
    # the pool child knows whether to store it over the child's blob.
    plan = await _intake_plan(mesg, parent)
    # A stored response that will reach the premerge (it has results, which
    # only the pool child knows) is premerged in the same pool call, so it is
    # decoded and encoded once. Nothing else is: a failed query is not
    # merged, and a child whose parent is gone is not handed anywhere.
    premerge = None
    if plan == "store" and failed_code is None and parent is not None:
        premerge = {
            "agent_name": agent_name,
            "inforesid": inforesid,
            "do_validate": _do_validate(mesg),
            "annotate": _annotate_for(agent_name),
        }
    # the pool child continues this trace under the current task span
    carrier: Dict[str, str] = {}
    inject(carrier)
    try:
        # Decoding, the result count, the score stats and the premerge all
        # run in the pool child: a multi-MB response is never resident here.
        outcome = await _run_intake_in_pool(
            response_id,
            child_pk,
            plan == "store",
            failed_code is None,
            premerge,
            carrier,
            logger.getEffectiveLevel(),
        )
    except KeyError as e:
        # finish_query only hands over a response the ARA pipeline finished,
        # so a missing blob means it was lost (e.g. Redis restarted and
        # reloaded an older snapshot), not that it is still coming. Nothing
        # will ever re-deliver it: fail the child now instead of leaving it
        # Running until the watchdog times it out.
        logger.error(f"Intake: no response blob {response_id} for {child_pk}: {e}")
        if mesg["status"] in ("D", "E"):
            # already terminal (e.g. the watchdog got there first)
            return None
        await _terminal_error(
            child_pk, mesg.get("ref"), mesg, None, logger, have_response=False
        )
        return None
    except Exception as e:
        logger.error(f"Intake failed for {child_pk}: {e}", exc_info=True)
        await _terminal_error(child_pk, mesg.get("ref"), mesg, None, logger)
        return None
    # the premerge stage reports, into this query's logs
    ingest_query_logs(logger, outcome.get("logs"))
    result_length = outcome["result_count"]
    result_stat = outcome["result_stat"]
    try:
        if parent is None:
            logger.error(f"Intake: unknown parent for child {child_pk}")
            return None
        await notify_subscribers(
            parent,
            {
                "event_type": "ara_response_complete",
                "ara_name": inforesid,
                "child_uuid": str(mesg["id"]),
                "ara_response_status": status,
                "ara_n_results": result_length,
            },
            logger,
        )
        logger.info(
            f"received msg from agent: {inforesid} with parent pk: "
            f"{mesg['ref']} and result: {result_length}"
        )
        if plan == "skip_done":
            logger.info(f"Intake: {child_pk} already has results; skipping")
            return None
        if plan == "skip_terminal":
            logger.info(f"Intake: {child_pk} already has a response; skipping")
            return None
        if plan == "finish_status":
            # Premerge and the hand-off to the merge are done; only the
            # child's own status flip was lost.
            logger.warning(
                f"Intake: {child_pk} was already handed to the merge; "
                "finishing its status"
            )
            updated = await ars_db.update_message(child_pk, status=status, code=200)
            await ars_db.persist_child_copy(mesg, logger)
            if updated and updated["status"] in ("D", "S", "E", "U"):
                await lifecycle.check_parent_completion(mesg["ref"], logger)
            return None
        if plan == "skip_errored":
            logger.info(f"Intake: {child_pk} already errored; response rejected")
            return None
        if mesg.get("result_count"):
            # Resuming: the premerge ran again from the ARA's own response,
            # not over the child's blob, so whatever the dead worker had
            # already applied is not applied twice.
            logger.warning(f"Intake: {child_pk} was redelivered; resuming its premerge")
        if failed_code is not None:
            # Terminal inline: a failed query's results (if any made it into
            # the response before the failure) are not merged, as upstream
            # would not merge a non-200 answer. The worker's reason is in the
            # ARA query's logs (served with its archived response once the
            # child's own copy expires) and in the line below, on this query.
            logger.info(
                f"Intake: {child_pk} ARA query ended {ara_status}"
                + (
                    f" ({fields['ara_description']})"
                    if fields.get("ara_description")
                    else ""
                )
                + f"; child errored with code {failed_code}"
            )
            updated = await ars_db.update_message(
                child_pk, status="E", code=failed_code
            )
            await ars_db.persist_child_copy(mesg, logger)
            if updated and updated["status"] in ("D", "S", "E", "U"):
                await lifecycle.check_parent_completion(mesg["ref"], logger)
            return None
        if result_length:
            await ars_db.update_message(
                child_pk, result_count=result_length, result_stat=result_stat
            )
            if outcome.get("error") is not None:
                # The premerge stages failed in the pool child (the payload,
                # as far as they got, is saved under the child): upstream's
                # generic handler, after the notification and the counts, as
                # when premerge was a pool call of its own.
                logger.error(f"premerge failed for {child_pk}: {outcome['error']}")
                await _terminal_error(child_pk, mesg.get("ref"), mesg, None, logger)
                return None
            fields["child_pk"] = str(child_pk)
            fields["agent_name"] = agent_name
            fields["inforesid"] = str(inforesid or "")
            fields["status"] = status
            return {"valid": outcome["valid"]}
        # no results: terminal inline, nothing to premerge or validate
        updates = {"status": status, "code": 200}
        if result_length is None:
            updates["result_count"] = 0
        updated = await ars_db.update_message(child_pk, **updates)
        await ars_db.persist_child_copy(mesg, logger)
        if updated and updated["status"] in ("D", "S", "E", "U"):
            await lifecycle.check_parent_completion(mesg["ref"], logger)
        return None
    except Exception as e:
        logger.error(f"Intake failed for {child_pk}: {e}", exc_info=True)
        await _terminal_error(child_pk, mesg.get("ref"), mesg, None, logger)
        return None


async def _intake_plan(mesg, parent) -> str:
    """What an intake does with the response, from the child's row alone.

    ``store`` saves it over the child's blob and carries on; the rest leave
    the blob alone: ``skip_done`` / ``skip_terminal`` / ``skip_errored`` are
    the endpoint's guards, and ``finish_status`` is a redelivery whose
    premerge and merge hand-off already happened.
    """
    if mesg["status"] == "D":
        return "skip_done"
    if mesg.get("result_count") is not None and mesg["result_count"] > 0:
        # Intake records the count before the premerge runs, so a redelivery
        # of this task (its worker died part way) lands here too. Skipping it
        # left the child Running until the watchdog timed it out.
        if mesg["status"] != "R":
            return "skip_terminal"
        if parent is not None and await _handed_to_merge(parent, mesg):
            return "finish_status"
        return "store"
    if mesg["status"] == "E":
        return "skip_errored"
    return "store"


def intake_in_child(
    response_id,
    child_pk,
    save,
    score,
    premerge=None,
    otel_carrier=None,
    log_level=logging.INFO,
) -> Dict[str, Any]:
    """Pool-side intake (and premerge): load the ARA's response once, count
    its results and, when ``save``, store it under the child's pk once.

    ``premerge`` (``{agent_name, inforesid, do_validate, annotate}``, or
    None) asks for the premerge stages to run on a response with results
    before it is stored, so the response is decoded and encoded once rather
    than once per stage. A stored response that is not premerged (no
    results, a failed query, no premerge asked for) is stored as the ARA
    sent it.

    Returns ``{result_count, result_stat, valid, error, logs}``: the count
    is None when the response has no results list; the stats are only
    computed for a stored response with results whose query didn't fail
    (``score``), from its scores as sent; ``valid`` is the premerge verdict
    (None when no premerge ran); ``error`` describes a premerge stage
    failure, in which case the payload is stored as far as the stages got
    (as upstream's generic handler saved the data it was holding); ``logs``
    are the stage reports for the worker to fold into the query's logs.
    Raises KeyError when the response blob is gone.
    """
    data = get_message_sync(str(response_id))
    res = get_safe(data, "message", "results")
    result_length = len(res) if res is not None else None
    result_stat = ScoreStatCalc(res) if save and score and result_length else None
    outcome = {
        "result_count": result_length,
        "result_stat": result_stat,
        "valid": None,
        "error": None,
        "logs": [],
    }
    if not save:
        return outcome
    if premerge is not None and score and result_length:
        setup_pool_child_tracer(STREAM)
        parent_ctx = extract(otel_carrier) if otel_carrier else None
        with pool_task_logger(STREAM, log_level) as (report, handler):
            with tracer.start_as_current_span(
                "ars.premerge.process", context=parent_ctx
            ) as span:
                span.set_attribute("agent", premerge["agent_name"])
                span.set_attribute("premerge.child_pk", str(child_pk))
                span.set_attribute("premerge.validate", bool(premerge["do_validate"]))
                try:
                    valid = _premerge_stages(
                        data,
                        child_pk,
                        premerge["agent_name"],
                        premerge["inforesid"],
                        premerge["do_validate"],
                        premerge["annotate"],
                        report,
                    )
                    outcome["valid"] = valid
                    span.set_attribute(
                        "premerge.annotate", bool(premerge["annotate"] and valid)
                    )
                    span.set_attribute("premerge.valid", valid)
                except Exception as e:
                    # Caught rather than raised, so the intake's own steps
                    # (the notification, the counts) still happen before the
                    # child fails, as when premerge was a pool call of its
                    # own. The traceback goes to the console.
                    logging.getLogger(__name__).exception(
                        f"premerge failed for {child_pk}"
                    )
                    outcome["error"] = f"{type(e).__name__}: {e}"
            outcome["logs"] = handler.drain()
    save_message_sync(str(child_pk), data)
    return outcome


async def _run_intake_in_pool(
    response_id, child_pk, save, score, premerge, otel_carrier, log_level
):
    """Indirection for tests; production runs intake_in_child in the pool."""
    args = (
        str(response_id),
        str(child_pk),
        save,
        score,
        premerge,
        otel_carrier,
        log_level,
    )
    if _pool is not None and _loop is not None:
        return await _pool.run(_loop, intake_in_child, *args)
    return await asyncio.to_thread(intake_in_child, *args)


async def _handed_to_merge(parent, mesg) -> bool:
    """Whether ``mesg`` has already been handed to its parent's merge: it is
    waiting in the merge-ready index, or the merge has folded it in (the
    parent's merged_versions_list names its agent -- one child per ARA)."""
    if await ars_db.is_ready_child(parent["id"], mesg["id"]):
        return True
    merged = parent.get("merged_versions_list") or []
    return any(
        len(entry) > 1 and entry[1] == str(mesg.get("agent")) for entry in merged
    )


async def _hand_to_merge(parent_pk, child_pk, agent_name, otel, logger):
    """Record the child as merge-ready and wake the merge worker.

    The index entry is what gets the child merged; the wake task is only a
    hint (the worker holding the parent's lock drains the index in arrival
    order). Both are attempted strictly: a child that validated but can
    never be merged would leave its parent waiting on a merge child that
    never comes, so the caller fails the child instead.
    """
    await ars_db.add_ready_child(parent_pk, child_pk, logger)
    await add_task(
        "ars.merge",
        {
            "parent_pk": str(parent_pk),
            "child_pk": str(child_pk),
            "agent_name": agent_name,
            "query_id": str(parent_pk),
            "otel": otel,
        },
        logger,
        raise_on_failure=True,
    )


async def ars_premerge(task, logger: logging.Logger):
    verdict = None
    if "intake_child_pk" in task[1]:
        # A delivery: the intake premerged the response in its one pool call
        # and hands back the verdict. Otherwise the task names a child whose
        # payload is already stored, premerged here.
        verdict = await intake_internal_response(task[1], logger)
        if verdict is None:
            return
    child_pk = task[1]["child_pk"]
    parent_pk = task[1]["parent_pk"]
    agent_name = task[1]["agent_name"]
    inforesid = task[1].get("inforesid") or None
    # carried from the callback's tr_ars.message.status header; re-clamped
    # here because the task payload is just as untrusted as the header was
    status = coerce_status(task[1].get("status", "D"), "D")

    mesg = await ars_db.get_message_row(child_pk)
    if mesg is None:
        logger.error(f"Premerge: child {child_pk} not found")
        return
    parent = await ars_db.get_message_row(parent_pk)

    if verdict is not None:
        valid = verdict["valid"]
    else:
        # the pool child continues this trace under the current task span
        carrier: Dict[str, str] = {}
        inject(carrier)
        try:
            outcome = await _run_premerge_in_pool(
                str(child_pk),
                agent_name,
                inforesid,
                _do_validate(mesg),
                carrier,
                logger,
            )
        except Exception as e:
            logger.error(f"premerge failed for {child_pk}: {e}", exc_info=True)
            await _terminal_error(child_pk, parent_pk, mesg, None, logger)
            return
        # the premerge stage reports, into this query's logs
        ingest_query_logs(logger, outcome.get("logs"))
        valid = outcome["valid"]

    if valid:
        if agent_name.startswith("ara-"):
            # merge before the child's status flip, matching upstream's
            # apply_async-then-save ordering
            try:
                await _hand_to_merge(
                    parent_pk, child_pk, agent_name, task[1].get("otel", "{}"), logger
                )
            except Exception as e:
                logger.error(
                    f"Could not hand {child_pk} to the merge worker: {e}",
                    exc_info=True,
                )
                await _terminal_error(child_pk, parent_pk, mesg, None, logger)
                return
        updated = await ars_db.update_message(child_pk, status=status, code=200)
        await ars_db.persist_child_copy(mesg, logger)
        if updated and updated["status"] in ("D", "S", "E", "U"):
            await lifecycle.check_parent_completion(parent_pk, logger)
    else:
        logger.debug(
            f"Validation problem found for agent {agent_name} with pk {parent_pk}"
        )
        await ars_db.update_message(child_pk, status="E", code=422)
        await ars_db.persist_child_copy(mesg, logger)
        if parent is not None:
            await notify_subscribers(
                parent,
                {
                    "event_type": "ara_failed_validation",
                    "ara_name": inforesid,
                    "child_uuid": str(mesg["id"]),
                    "ara_response_status": "E",
                    # the count the intake recorded (upstream: len(results)
                    # of the payload it was holding; premerge only runs
                    # when there are results, so they are the same number)
                    "ara_n_results": mesg.get("result_count"),
                },
                logger,
            )
        await lifecycle.check_parent_completion(parent_pk, logger)


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    with tracer.start_as_current_span(STREAM, context=parent_ctx):
        try:
            await ars_premerge(task, logger)
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
    # Sized by the pod's CPU allocation: each in-flight task runs one pool
    # child holding one ARA response, so pool size == concurrency bounds
    # both CPU and peak memory. POOL_MAX_WORKERS overrides.
    max_workers = resolve_pool_workers(TASK_LIMIT, LOGGER)
    LOGGER.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    _pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="ars_premerge process pool",
        task_timeout=settings.pool_task_timeout_sec,
        warmup=_warm_pool_child if settings.pool_prewarm else None,
    )
    await run_poll_loop(
        STREAM, GROUP, CONSUMER, max_workers, process_task, on_stop=_pool.shutdown
    )


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
