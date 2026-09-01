"""Parent-completion orchestration.

The Shepherd equivalent of the upstream post_save signal's completion block
(signals.py message_post_save): called by every code path that moves a child
message into a terminal status. Evaluates the ported counting rules and
applies the transitions, notifications, and empty-merge synthesis.
"""

import logging
from typing import Any, Dict

from . import db as ars_db
from .completion import evaluate_completion
from .notify import notify_subscribers

logger = logging.getLogger(__name__)

# The three built-in actors (api.py DEFAULT_ACTOR / WORKFLOW_ACTOR /
# ARS_ACTOR). get_or_create semantics match upstream's lazy creation.
DEFAULT_ACTOR = {
    "channel": ["general"],
    "agent": {"name": "ars-default-agent", "uri": ""},
    "path": "",
    "inforesid": "",
}
WORKFLOW_ACTOR = {
    "channel": ["workflow"],
    "agent": {"name": "ars-workflow-agent", "uri": ""},
    "path": "",
    "inforesid": "",
}
ARS_ACTOR = {
    "channel": [],
    "agent": {"name": "ars-ars-agent", "uri": ""},
    "path": "",
    "inforesid": "infores:ars",
}

MERGE_AGENT_NAME = "ars-ars-agent"


async def ensure_default_actor() -> Dict[str, Any]:
    actor, _ = await ars_db.get_or_create_actor(DEFAULT_ACTOR)
    return actor


async def ensure_workflow_actor() -> Dict[str, Any]:
    actor, _ = await ars_db.get_or_create_actor(WORKFLOW_ACTOR)
    return actor


async def ensure_ars_actor() -> Dict[str, Any]:
    actor, _ = await ars_db.get_or_create_actor(ARS_ACTOR)
    return actor


def _child_record(child: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": child["status"],
        "code": child["code"],
        "agent_name": child["agent_name"],
        "result_count": child.get("result_count"),
    }


async def check_parent_completion(parent_pk, task_logger: logging.Logger) -> None:
    """Evaluate and apply the upstream parent-completion transitions."""
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
            # Synthesize the empty merged message from the parent's data with
            # results / kg / aux emptied (signals.py lines 82-104).
            ars_actor = await ensure_ars_actor()
            empty = await ars_db.create_message(
                actor_id=ars_actor["id"],
                status="Running",
                code=202,
                ref=parent_pk,
            )
            empty_data = await ars_db.load_message_data(parent_pk, task_logger)
            if not isinstance(empty_data, dict):
                empty_data = {}
            message = empty_data.setdefault("message", {})
            message["results"] = []
            message["auxiliary_graphs"] = {}
            message["knowledge_graph"] = {"nodes": {}, "edges": {}}
            await ars_db.save_message_data(empty["id"], empty_data, task_logger)
            await ars_db.update_message(
                empty["id"], skip_coercion=True, status="D", code=200
            )
            await ars_db.persist_data_copy(empty["id"], task_logger)
            updated = await ars_db.update_message(
                parent_pk,
                status="D",
                code=200,
                merged_version=str(empty["id"]),
                merged_versions_list=[[str(empty["id"]), "ars"]],
            )
            # parent.save() still notifies ('D' -> admin/complete); the empty
            # branch does NOT clear subscriptions upstream.
            await notify_subscribers(updated, None, task_logger)
        else:
            # one last notification about the final merge, built while the
            # parent is still Running so the custom fields survive
            await notify_subscribers(
                parent,
                {
                    "event_type": "last_merged_completed",
                    "complete": True,
                    "merged_versions_list": (
                        parent.get("merged_versions_list")
                        if parent.get("merged_versions_list") is not None
                        else []
                    ),
                },
                task_logger,
            )
            updated = await ars_db.update_message(parent_pk, status="D", code=200)
            # save-time notification: parent now 'D' -> admin/complete
            await notify_subscribers(updated, None, task_logger)
            await ars_db.clear_subscriptions(parent_pk)
        await ars_db.persist_data_copy(parent_pk, task_logger)
    elif parent["status"] == "E":
        await ars_db.clear_subscriptions(parent_pk)
