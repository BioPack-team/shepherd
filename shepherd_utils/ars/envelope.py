"""Django-serializer-compatible wire envelopes.

The upstream ARS returns Django's ``serializers.serialize('json', [obj])``
shape from most endpoints: ``{"model": "tr_ars.<model>", "pk": ..., "fields":
{...}}`` with datetimes encoded by ``DjangoJSONEncoder`` (ISO 8601, fractional
seconds truncated to milliseconds, ``+00:00`` spelled ``Z``). The UI and the
upstream smoke tests depend on this exact shape, so the port reproduces it
field-for-field, in Django model declaration order.

Ported from NCATSTranslator/Relay @ dd1e71b: tr_sys/tr_ars/models.py.
"""

import datetime
from typing import Any, Dict, List, Optional, Union

from .statuses import to_name


def django_datetime(dt: Optional[datetime.datetime]) -> Optional[str]:
    """Format a datetime exactly like DjangoJSONEncoder.

    isoformat, with microseconds truncated to milliseconds and a trailing
    ``+00:00`` rewritten as ``Z``.
    """
    if dt is None:
        return None
    r = dt.isoformat()
    if dt.microsecond:
        r = r[:23] + r[26:]
    if r.endswith("+00:00"):
        r = r[:-6] + "Z"
    return r


def _opt_str(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def message_envelope(
    row: Dict[str, Any],
    data: Optional[Union[Dict, List]] = None,
) -> Dict[str, Any]:
    """Serialize an ars_message row the way ``Message.to_dict`` does.

    ``data`` is the already-decompressed payload dict; upstream ``to_dict``
    only replaces ``fields.data`` when the stored blob is not None, so pass
    ``None`` to render a message without a payload.
    """
    return {
        "model": "tr_ars.message",
        "pk": str(row["id"]),
        "fields": {
            "name": row.get("name", ""),
            "code": row.get("code"),
            "status": to_name(row.get("status")),
            "actor": row.get("actor"),
            "timestamp": django_datetime(row.get("ts")),
            "updated_at": django_datetime(row.get("updated_at")),
            "data": data,
            "url": row.get("url"),
            "ref": _opt_str(row.get("ref")),
            "result_count": row.get("result_count"),
            "result_stat": row.get("result_stat"),
            "retain": row.get("retain", False),
            "merge_semaphore": row.get("merge_semaphore", False),
            "merged_version": _opt_str(row.get("merged_version")),
            "merged_versions_list": row.get("merged_versions_list"),
            "params": row.get("params"),
            "clients": row.get("clients", []),
        },
    }


def agent_envelope(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "model": "tr_ars.agent",
        "pk": row["id"],
        "fields": {
            "name": row.get("name"),
            "description": row.get("description"),
            "uri": row.get("uri"),
            "contact": row.get("contact"),
            "registered": django_datetime(row.get("registered")),
            "updated": django_datetime(row.get("updated")),
        },
    }


def channel_envelope(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "model": "tr_ars.channel",
        "pk": row["id"],
        "fields": {
            "name": row.get("name"),
            "description": row.get("description"),
        },
    }


def actor_envelope(
    row: Dict[str, Any],
    agent_uri: str = "",
) -> Dict[str, Any]:
    """Serialize an ars_actor row like ``Actor.to_dict``.

    The stored ``channel`` JSON is the Django-serialized channel list (see
    ars db ``get_or_create_actor``), passed through verbatim; ``fields.url``
    is ``agent.uri + path`` exactly as ``Actor.url()`` computes it.
    """
    return {
        "model": "tr_ars.actor",
        "pk": row["id"],
        "fields": {
            "channel": row.get("channel", []),
            "agent": row.get("agent"),
            "path": row.get("path", ""),
            "inforesid": row.get("inforesid", ""),
            "active": row.get("active", True),
            "url": f"{agent_uri}{row.get('path', '')}",
        },
    }
