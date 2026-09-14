"""Subscriber notifications.

Ported from NCATSTranslator/Relay @ 3e65975:
  - models.py Message.notify_subscribers: 'D'/'E' parents override any custom
    event fields with admin/ars_error, and stats attach when the message has
    a result_count.
  - tasks.py notify_subscribers_task / notify_one_client_task: the payload
    base {pk, timestamp, code}, the last_merged_completed code forcing, and
    the per-client HMAC-signed delivery happen in the ars_notify worker; this
    module builds the fields and enqueues the wake task.
"""

import json
import logging
from typing import Any, Dict, Optional

import shepherd_utils.ars.db as ars_db
from shepherd_utils.broker import add_task

logger = logging.getLogger(__name__)


def build_notification(
    parent_row: Dict[str, Any],
    additional_fields: Optional[Dict[str, Any]],
    data: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Message.notify_subscribers field logic (sans the task dispatch)."""
    if parent_row.get("status") == "D":
        additional_fields = {"event_type": "admin", "complete": True}
    if parent_row.get("status") == "E":
        additional_fields = {
            "event_type": "ars_error",
            "message": "ARS has run into an Error",
            "complete": True,
        }
    if parent_row.get("result_count") is not None:
        try:
            aux_graphs = data["message"]["auxiliary_graphs"]
            aux_count = len(aux_graphs) if aux_graphs is not None else 0
        except Exception:
            logger.debug("Problem getting aux graphs for stats notification")
            aux_count = 0
        if additional_fields is None:
            # upstream raises a TypeError here; be tolerant and carry the
            # stats alone (documented deviation, see the parity register).
            additional_fields = {}
        additional_fields["stats"] = {
            "results": parent_row["result_count"],
            "auxiliary_graphs": aux_count,
        }
    return additional_fields


async def notify_subscribers(
    message_row: Dict[str, Any],
    additional_fields: Optional[Dict[str, Any]],
    logger: logging.Logger,
    data: Optional[Dict[str, Any]] = None,
    client_pk: Optional[int] = None,
) -> None:
    """Build the notification fields and wake the ars_notify worker.

    Recipients are resolved HERE, at emit time, and carried in the task as
    ``client_pks``: the completion path clears a parent's subscriptions
    right after emitting its final events, so a worker that looked the
    subscribers up later would find nobody. ``client_pk`` addresses one
    client explicitly (a late subscriber to a finished query, see
    ``replay_completion``). If the subscriber lookup itself fails the task
    is still enqueued without a recipient list and the worker falls back to
    looking them up."""
    fields = build_notification(message_row, additional_fields, data=data)
    try:
        # rejoin the query's submit-time trace (the row is normally the
        # query parent; fall back through ref for a child row)
        query_pk = message_row.get("ref") or message_row["id"]
        payload = {
            "message_pk": str(message_row["id"]),
            "query_id": str(message_row["id"]),
            "code": str(message_row.get("code", 200)),
            "fields": json.dumps(fields) if fields is not None else "null",
            "otel": await ars_db.load_otel_carrier(query_pk, logger),
        }
        if client_pk is not None:
            payload["client_pks"] = json.dumps([str(client_pk)])
        else:
            try:
                clients = await ars_db.get_subscribed_clients(message_row["id"])
                payload["client_pks"] = json.dumps([str(c["id"]) for c in clients])
            except Exception as e:
                logger.warning(
                    f"Could not resolve subscribers for {message_row['id']} at emit "
                    f"time; the notify worker will look them up: {e}"
                )
        await add_task("ars.notify", payload, logger)
    except Exception as e:
        logger.error(f"Failed to enqueue notification for {message_row['id']}: {e}")


def _has_real_merge(message_row: Dict[str, Any]) -> bool:
    """True when the parent finished with at least one ARA merge. The
    empty-completion branch records only [[pk, "ars"]] and upstream does not
    emit last_merged_completed for it."""
    for item in message_row.get("merged_versions_list") or []:
        try:
            agent = item[1]
        except (IndexError, TypeError, KeyError):
            continue
        if agent != "ars":
            return True
    return False


async def replay_completion(
    message_row: Dict[str, Any], client_pk: int, logger: logging.Logger
) -> None:
    """Deliver, to one client, the notifications a live subscriber would have
    received when this already-terminal message completed.

    The response cache hands a Done pk straight back from /submit, so the
    client's subscription arrives after every completion event has already
    fired (and upstream would refuse it as "Query already complete"). The
    same events are re-emitted in the same order: for a Done parent with a
    real merge, ``last_merged_completed`` (built against a Running-shaped
    row exactly as the live path does, so the custom fields survive the
    'D' override) and then the save-time admin/complete; an Error message
    yields ars_error via the same override rules. The events are addressed
    to ``client_pk`` alone: the message's own subscriber list was cleared
    when it completed.
    """
    status = message_row.get("status")
    if (
        status == "D"
        and message_row.get("ref") is None
        and _has_real_merge(message_row)
    ):
        await notify_subscribers(
            dict(message_row, status="R"),
            {
                "event_type": "last_merged_completed",
                "complete": True,
                "merged_versions_list": message_row.get("merged_versions_list") or [],
            },
            logger,
            client_pk=client_pk,
        )
    await notify_subscribers(message_row, None, logger, client_pk=client_pk)
