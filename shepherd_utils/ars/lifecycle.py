"""Parent-completion orchestration.

The Shepherd equivalent of the upstream post_save signal's completion block
(signals.py message_post_save): called by every code path that moves a child
message into a terminal status. Evaluates the ported counting rules and
applies the transitions, notifications, and empty-merge synthesis.
"""

import copy
import logging
from typing import Any, Dict

import shepherd_utils.db as shepherd_db
from shepherd_utils.trapi import finalize_response

from . import aras
from . import cache
from . import db as ars_db
from .completion import evaluate_completion
from .notify import notify_subscribers

logger = logging.getLogger(__name__)

MERGE_AGENT_NAME = aras.MERGE_AGENT


def empty_merged_response(query: Any) -> Dict[str, Any]:
    """The merged message a query with no results completes with.

    Upstream (signals.py lines 82-104) re-saved the parent's data -- the
    submitted QUERY -- with results / knowledge graph / auxiliary graphs
    emptied, so the "response" still carried the query's top-level members
    (``workflow``, ``submitter``, ``parameters`` as a query parameter ...)
    and an ``auxiliary_graphs: {}`` TRAPI 2.0 forbids. This builds a TRAPI
    2.0 Response instead: the query's query graph, an empty knowledge graph,
    ``results: []``, the version stamps, and the query's ``parameters``
    echoed back as 2.0 requires (``finalize_response``).
    """
    if not isinstance(query, dict):
        query = {}
    query_message = query.get("message")
    if not isinstance(query_message, dict):
        query_message = {}
    message: Dict[str, Any] = {}
    query_graph = query_message.get("query_graph")
    if isinstance(query_graph, dict) and query_graph:
        message["query_graph"] = copy.deepcopy(query_graph)
    message["knowledge_graph"] = {"nodes": {}, "edges": {}}
    message["results"] = []
    return dict(finalize_response({"message": message}, query))


def _child_record(child: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": child["status"],
        "code": child["code"],
        "agent_name": child["agent_name"],
        "result_count": child.get("result_count"),
    }


async def _discard_empty_merge(empty_pk, task_logger: logging.Logger) -> None:
    """Drop an empty merged message built by a caller that lost the claim."""
    try:
        await ars_db.delete_message(empty_pk)
    except Exception as e:
        task_logger.warning(f"Could not delete orphan empty merge {empty_pk}: {e}")
    try:
        await shepherd_db.data_db_client.delete(str(empty_pk))
    except Exception:
        pass


async def check_parent_completion(parent_pk, task_logger: logging.Logger) -> None:
    """Evaluate and apply the upstream parent-completion transitions.

    Called from the server's callback handler and from four workers, so two
    children going terminal at once can bring two callers here with the same
    complete decision. The parent's flip to 'D' is therefore claimed with a
    conditional UPDATE (``claim_terminal_transition``) before any of the
    completion work runs: exactly one caller proceeds, and the losers return
    without re-notifying subscribers or synthesizing a second merged message.
    """
    parent = await ars_db.get_message_row(parent_pk)
    if parent is None:
        task_logger.warning(f"Completion check: parent {parent_pk} not found")
        return
    if parent["status"] == "D":
        return
    children = await ars_db.get_children(parent_pk)
    decision = evaluate_completion([_child_record(c) for c in children])
    task_logger.info(
        f"Completion check for {parent_pk}: finished={decision.finished} "
        f"merge_count={decision.merge_count} orig_count={decision.orig_count}"
    )
    if decision.complete:
        if decision.empty:
            # Synthesize the empty merged message from the parent's query
            # (signals.py lines 82-104; a TRAPI 2.0 Response here, see
            # empty_merged_response). Built
            # BEFORE the claim so the parent can go Done with its
            # merged_version already set -- a reader must never catch it 'D'
            # but unmerged. A caller that then loses the claim drops the
            # message it just built.
            empty = await ars_db.create_message(
                agent=aras.MERGE_AGENT,
                status="Running",
                code=202,
                ref=parent_pk,
            )
            query = await ars_db.load_message_data(parent_pk, task_logger)
            empty_data = empty_merged_response(query)
            await ars_db.save_message_data(empty["id"], empty_data, task_logger)
            await ars_db.update_message(
                empty["id"], skip_coercion=True, status="D", code=200
            )
            await ars_db.persist_data_copy(empty["id"], task_logger)
            claimed = await ars_db.claim_terminal_transition(
                parent_pk,
                "D",
                200,
                merged_version=str(empty["id"]),
                merged_versions_list=[[str(empty["id"]), "ars"]],
            )
            if claimed is None:
                task_logger.debug(
                    f"Completion for {parent_pk} already claimed elsewhere; "
                    f"discarding the empty merge {empty['id']}"
                )
                await _discard_empty_merge(empty["id"], task_logger)
                return
            updated = claimed
            # parent.save() still notifies ('D' -> admin/complete); the empty
            # branch does NOT clear subscriptions upstream.
            await notify_subscribers(claimed, None, task_logger)
        else:
            # The merged_version is already on the row (ars_merge set it), so
            # the claim only has to flip the status.
            claimed = await ars_db.claim_terminal_transition(parent_pk, "D", 200)
            if claimed is None:
                task_logger.debug(
                    f"Completion for {parent_pk} already claimed elsewhere; skipping"
                )
                return
            updated = claimed
            # one last notification about the final merge, built against the
            # pre-flip shape so its custom fields survive the 'D' override
            await notify_subscribers(
                dict(claimed, status="R"),
                {
                    "event_type": "last_merged_completed",
                    "complete": True,
                    "merged_versions_list": (
                        claimed.get("merged_versions_list")
                        if claimed.get("merged_versions_list") is not None
                        else []
                    ),
                },
                task_logger,
            )
            # save-time notification: parent now 'D' -> admin/complete
            await notify_subscribers(claimed, None, task_logger)
            await ars_db.clear_subscriptions(parent_pk)
        await ars_db.persist_data_copy(parent_pk, task_logger)
        # Response cache: a leader publishes its tree (or drops its pending
        # entry when the run is not cacheable); an overwrite run repoints
        # its key. No-op otherwise.
        if updated is not None:
            await cache.on_parent_complete(updated, task_logger)
    elif parent["status"] == "E":
        await ars_db.clear_subscriptions(parent_pk)
        await cache.on_parent_failed(parent, task_logger)
