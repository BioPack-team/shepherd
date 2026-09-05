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
) -> None:
    """Build the notification fields and wake the ars_notify worker."""
    fields = build_notification(message_row, additional_fields, data=data)
    try:
        await add_task(
            "ars.notify",
            {
                "message_pk": str(message_row["id"]),
                "query_id": str(message_row["id"]),
                "code": str(message_row.get("code", 200)),
                "fields": json.dumps(fields) if fields is not None else "null",
                "otel": "{}",
            },
            logger,
        )
    except Exception as e:
        logger.error(f"Failed to enqueue notification for {message_row['id']}: {e}")
