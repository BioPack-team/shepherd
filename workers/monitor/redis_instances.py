"""Which Redis servers the monitor watches, and how it names them.

Shepherd uses Redis for four roles (broker, data, lock, logs). By default one
server holds all four; each role can be moved onto a Redis of its own, and any
of them can sit behind Sentinel (shepherd_utils/redis_clients.py). The monitor
reports per *instance* -- per distinct server -- because memory, eviction,
persistence and restarts are properties of a server, not of a role.

One instance is special: the one that serves the broker role. It holds the
task streams, the worker heartbeats and the monitor's own state, and it is the
only Redis there was before roles could be split. Everything the monitor says
about it keeps the identity it always had (``snapshot["redis"]``, alert names
such as ``redis_memory_high``, history series such as
``redis:used_memory_bytes``), so alert routing and stored history carry on
unchanged. Every other instance gets the same names with the instance
appended: ``redis_memory_high@shepherd_data:6379``,
``redis@shepherd_data:6379:used_memory_bytes``.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis

from shepherd_utils import redis_clients
from shepherd_utils.config import parse_size_to_bytes, settings

BROKER = redis_clients.BROKER
DATA = redis_clients.DATA


def discover() -> List[Tuple[str, List[str]]]:
    """Every distinct Redis and the roles it serves, the broker role's first."""
    grouped = redis_clients.instances()
    return sorted(grouped.items(), key=lambda item: (BROKER not in item[1], item[0]))


# The monitor's own clients for instances other than the broker role's (that
# one is reached through ``shepherd_utils.broker.broker_client``, as ever).
# A small pool of its own, rather than the data/lock/logs pools the rest of
# the codebase shares, so an INFO never queues behind a payload transfer.
_extra_clients: Dict[str, aioredis.Redis] = {}


def extra_client(instance: str, roles: List[str]) -> aioredis.Redis:
    """The client for an instance that does not serve the broker role. Which
    of its roles the connection is opened for doesn't matter: INFO describes
    the whole server."""
    client = _extra_clients.get(instance)
    if client is None:
        client = redis_clients.async_client(
            roles[0],
            max_connections=2,
            timeout=5,
            socket_timeout=5,
            socket_connect_timeout=5,
            health_check_interval=30,
            decode_responses=True,
            retry_on_timeout=True,
        )
        _extra_clients[instance] = client
    return client


def uses_sentinel(roles: List[str]) -> bool:
    """Whether an instance (given the roles it serves) is found through
    Sentinel. Roles on one instance share one target, so the first decides."""
    return redis_clients.target(roles[0]).uses_sentinel


def memory_limit_bytes(roles: List[str]) -> int:
    """The container memory limit of the instance serving ``roles``; 0 when
    not configured, which skips the footprint side of the memory alert.

    ``redis_memory_limit`` has always meant "the broker's container", so it
    applies to whichever instance serves the broker role and to no other. An
    instance without the broker role takes ``redis_<role>_memory_limit`` of
    the first of its roles that has one.
    """
    if BROKER in roles:
        return settings.redis_memory_limit_bytes
    for role in roles:
        limit = parse_size_to_bytes(getattr(settings, f"redis_{role}_memory_limit", ""))
        if limit:
            return limit
    return 0


@dataclass(frozen=True)
class View:
    """One instance's stats from a snapshot, with how to name it in alerts."""

    stats: Dict[str, Any]
    # Serves the broker role: keeps the alert names and state there always were.
    primary: bool
    # More than one instance exists, so texts must say which one they mean.
    # With a single instance the wording stays what it was before the split.
    named: bool

    @property
    def instance(self) -> str:
        return self.stats.get("instance") or ""

    @property
    def roles(self) -> List[str]:
        """The roles served; every role for a snapshot that predates (or a
        test that omits) the per-instance tags."""
        roles = self.stats.get("roles")
        return list(redis_clients.ROLES) if roles is None else list(roles)

    @property
    def suffix(self) -> str:
        """Appended to alert names, cooldown keys and state keys. Empty for
        the broker role's instance; ``@<instance>`` for any other, which no
        existing name contains, so the two can never collide."""
        return "" if self.primary else f"@{self.instance}"

    def rule(self, base: str) -> str:
        return base + self.suffix

    @property
    def tag(self) -> str:
        """`` `instance` (roles)`` to put after the word "Redis"; empty when
        there is only one instance."""
        if not self.named:
            return ""
        return f" `{self.instance}` ({', '.join(self.roles)})"

    def name(self, legacy: str) -> str:
        """How to refer to this instance: ``legacy`` (the pre-split wording)
        when it is the only one, else "Redis `instance` (roles)"."""
        return f"Redis{self.tag}" if self.named else legacy

    def event_fields(self) -> Dict[str, Any]:
        """Extra fields for an alert event about this instance. None in the
        single-instance case, so those events are exactly what they were."""
        if not self.named:
            return {}
        return {"instance": self.instance, "roles": self.roles}


def views(snapshot: Dict[str, Any]) -> List[View]:
    """Every instance in ``snapshot``, the broker role's first.

    A snapshot without ``redis_instances`` (one built before the per-instance
    view existed) is its ``redis`` entry alone.
    """
    per_instance: Optional[Dict[str, Dict[str, Any]]] = snapshot.get("redis_instances")
    if not per_instance:
        return [View(snapshot.get("redis") or {}, primary=True, named=False)]
    named = len(per_instance) > 1
    found = [
        View(
            stats,
            primary=stats.get("roles") is None or BROKER in stats["roles"],
            named=named,
        )
        for stats in per_instance.values()
    ]
    return sorted(found, key=lambda view: not view.primary)


def primary_view(snapshot: Dict[str, Any]) -> View:
    """The broker role's instance."""
    return views(snapshot)[0]
