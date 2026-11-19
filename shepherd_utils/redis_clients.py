"""Where Shepherd's Redis clients come from.

Shepherd uses Redis for four jobs, each with its own client:

* ``BROKER`` -- the task streams, worker heartbeats and the monitor's state;
* ``DATA``   -- query, response and callback payloads while they are hot;
* ``LOCK``   -- the per-query and per-parent merge locks;
* ``LOGS``   -- each query's log list.

By default all four are one Redis (``redis_host``), told apart by database
number (0-3), as they always were. Each role can instead be pointed at a
Redis of its own (``redis_<role>_host`` / ``_port`` / ``_password``), so the
small, latency-critical broker and locks stop sharing a command thread and
a ``maxmemory`` with multi-megabyte payload transfers (#252). A role given
its own Redis keeps its database number, so splitting is a configuration
change only.

Any of them can also be found through Redis Sentinel instead of by address
(#201): with ``redis_sentinels`` set, a role connects to whichever node
Sentinel currently names as the primary of its master (``redis_sentinel_master``,
or ``redis_<role>_sentinel_master`` for a role with a Redis of its own), and
follows it across a failover. Commands that fail because the old primary
went away or was demoted are retried against the new one.

What a role connects to, most specific first:

1. ``redis_<role>_sentinel_master`` (through ``redis_sentinels``);
2. ``redis_<role>_host`` (directly);
3. ``redis_sentinel_master`` (through ``redis_sentinels``);
4. ``redis_host`` (directly).

Every client in the codebase is built here, so these rules hold everywhere:
the async pools (shepherd_utils/broker.py, shepherd_utils/db.py) and the
sync clients the process-pool children and signal handlers use.
"""

import socket
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import redis
import redis.asyncio as aioredis
import redis.asyncio.sentinel as aiosentinel
import redis.sentinel as sync_sentinel
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry
from redis.asyncio.retry import Retry as AsyncRetry

from .config import settings

BROKER = "broker"
DATA = "data"
LOCK = "lock"
LOGS = "logs"

# A role's database number. Kept when the role moves to a Redis of its own.
ROLE_DB = {BROKER: 0, DATA: 1, LOCK: 2, LOGS: 3}
ROLES = tuple(ROLE_DB)

# A failover takes Sentinel ``down-after-milliseconds`` to notice plus a few
# seconds to promote; these retries only bridge the moment a connection finds
# its primary gone or demoted and is pointed at the new one. A caller that
# hits the outage itself still gets the error, and its own retry loop (or the
# worker's broker-health exit) deals with it as it does a broker restart.
FAILOVER_RETRIES = 3
FAILOVER_ERRORS = (RedisConnectionError, RedisTimeoutError, ReadOnlyError)


def keepalive_options() -> dict:
    """TCP keepalive tuning so a dead Redis connection is detected in ~tens of
    seconds instead of relying on Linux's 2-hour default keepalive idle time.

    Without these, a half-open connection -- the broker pod moved, or a stateful
    firewall/conntrack entry silently dropped the flow -- is only noticed via the
    per-command ``socket_timeout``, and reconnecting keeps hitting the same dead
    endpoint. Probing the peer actively lets the kernel tear the socket down so
    redis-py rebuilds it against a live endpoint. Only options the running
    platform defines are included (``TCP_KEEPIDLE`` and friends are Linux-only;
    macOS spells it ``TCP_KEEPALIVE``), so importing this on a dev laptop or in
    CI doesn't raise. Net effect on Linux: ~30s idle + 3 failed probes 10s apart
    => a dead connection is reaped in ~60s.
    """
    opts = {}
    if hasattr(socket, "TCP_KEEPIDLE"):
        opts[socket.TCP_KEEPIDLE] = 30
    if hasattr(socket, "TCP_KEEPINTVL"):
        opts[socket.TCP_KEEPINTVL] = 10
    if hasattr(socket, "TCP_KEEPCNT"):
        opts[socket.TCP_KEEPCNT] = 3
    return opts


def parse_sentinels(value: str) -> Tuple[Tuple[str, int], ...]:
    """``"host1:26379,host2"`` -> ``(("host1", 26379), ("host2", 26379))``.

    >>> parse_sentinels(" a:1, b ,")
    (('a', 1), ('b', 26379))
    >>> parse_sentinels("")
    ()
    """
    sentinels = []
    for item in (value or "").split(","):
        item = item.strip()
        if not item:
            continue
        host, _, port = item.rpartition(":")
        if host and port.isdigit():
            sentinels.append((host, int(port)))
        else:
            sentinels.append((item, 26379))
    return tuple(sentinels)


@dataclass(frozen=True)
class Target:
    """What one role connects to."""

    role: str
    db: int
    password: Optional[str]
    # direct
    host: str = ""
    port: int = 6379
    # through Sentinel (``sentinel_master`` set means Sentinel is used)
    sentinels: Tuple[Tuple[str, int], ...] = ()
    sentinel_master: str = ""

    @property
    def uses_sentinel(self) -> bool:
        return bool(self.sentinel_master)

    @property
    def instance(self) -> str:
        """Which Redis this is, whatever the role or database: roles with
        the same ``instance`` share one server (its memory, its thread, its
        persistence). The monitor reports per instance."""
        if self.uses_sentinel:
            return f"sentinel:{self.sentinel_master}"
        return f"{self.host}:{self.port}"


def target(role: str) -> Target:
    """Resolve ``role`` from the settings (see the module docstring for the
    order). Read at call time, so tests can change the settings."""
    if role not in ROLE_DB:
        raise ValueError(f"unknown Redis role {role!r}")
    own_host = getattr(settings, f"redis_{role}_host") or ""
    own_master = getattr(settings, f"redis_{role}_sentinel_master") or ""
    sentinels = parse_sentinels(settings.redis_sentinels)
    if own_host or own_master:
        # a Redis of its own: its port and password default to the shared ones
        port = getattr(settings, f"redis_{role}_port") or settings.redis_port
        password = getattr(settings, f"redis_{role}_password")
        if password is None:
            password = settings.redis_password
    else:
        port, password = settings.redis_port, settings.redis_password
    common: Dict[str, Any] = {"role": role, "db": ROLE_DB[role], "password": password}
    if own_master:
        if not sentinels:
            raise ValueError(
                f"redis_{role}_sentinel_master is set but redis_sentinels is empty"
            )
        return Target(sentinels=sentinels, sentinel_master=own_master, **common)
    if own_host:
        return Target(host=own_host, port=int(port), **common)
    if sentinels and settings.redis_sentinel_master:
        return Target(
            sentinels=sentinels,
            sentinel_master=settings.redis_sentinel_master,
            **common,
        )
    return Target(host=settings.redis_host, port=int(port), **common)


def instances() -> Dict[str, List[str]]:
    """Each distinct Redis and the roles it serves, e.g.
    ``{"shepherd_broker:6379": ["broker", "data", "lock", "logs"]}``."""
    grouped: Dict[str, List[str]] = {}
    for role in ROLES:
        grouped.setdefault(target(role).instance, []).append(role)
    return grouped


def _sentinel_kwargs() -> Dict[str, Any]:
    """How to talk to the Sentinels themselves (not the Redis they watch)."""
    kwargs: Dict[str, Any] = {"socket_timeout": 2, "socket_connect_timeout": 2}
    if settings.redis_sentinel_password:
        kwargs["password"] = settings.redis_sentinel_password
    return kwargs


class BlockingSentinelConnectionPool(
    aiosentinel.SentinelConnectionPool, aioredis.BlockingConnectionPool
):
    """A Sentinel-managed pool that waits for a free connection.

    redis-py's Sentinel pool raises as soon as ``max_connections`` are in
    use; every pool here is a ``BlockingConnectionPool`` precisely so a burst
    waits its turn instead (the direct pools above). This is that behaviour
    with Sentinel's primary discovery: connections are
    ``SentinelManagedConnection`` (they ask Sentinel where the primary is on
    every connect), handed out through the blocking pool's queue.
    """


def async_client(role: str, **pool_kwargs: Any) -> aioredis.Redis:
    """The async client for ``role``, on a blocking pool. ``pool_kwargs`` are
    the pool's and connections' options (``max_connections``, ``timeout``,
    ``socket_timeout``, ``decode_responses`` ...)."""
    return aioredis.Redis(connection_pool=async_pool(role, **pool_kwargs))


def async_pool(role: str, **pool_kwargs: Any) -> aioredis.ConnectionPool:
    """The blocking connection pool behind ``async_client``."""
    where = target(role)
    pool_kwargs.setdefault("socket_keepalive", True)
    pool_kwargs.setdefault("socket_keepalive_options", keepalive_options())
    if not where.uses_sentinel:
        return aioredis.BlockingConnectionPool(
            host=where.host,
            port=where.port,
            db=where.db,
            password=where.password,
            **pool_kwargs,
        )
    # ``retry_on_timeout`` is the direct pools' one-retry-on-timeout; here the
    # explicit Retry below covers it (and a vanished or demoted primary).
    pool_kwargs.pop("retry_on_timeout", None)
    sentinel = aiosentinel.Sentinel(
        list(where.sentinels), sentinel_kwargs=_sentinel_kwargs()
    )
    return BlockingSentinelConnectionPool(
        where.sentinel_master,
        sentinel,
        db=where.db,
        password=where.password,
        retry=AsyncRetry(ExponentialBackoff(cap=2, base=0.1), FAILOVER_RETRIES),
        retry_on_error=list(FAILOVER_ERRORS),
        **pool_kwargs,
    )


def sync_client(role: str, **kwargs: Any) -> redis.Redis:
    """A sync client for ``role`` (process-pool children, signal handlers).
    ``kwargs`` are connection options (``socket_timeout`` ...)."""
    where = target(role)
    if not where.uses_sentinel:
        return redis.Redis(
            host=where.host,
            port=where.port,
            db=where.db,
            password=where.password,
            **kwargs,
        )
    kwargs.pop("retry_on_timeout", None)
    sentinel = sync_sentinel.Sentinel(
        list(where.sentinels), sentinel_kwargs=_sentinel_kwargs()
    )
    return sentinel.master_for(
        where.sentinel_master,
        db=where.db,
        password=where.password,
        retry=Retry(ExponentialBackoff(cap=2, base=0.1), FAILOVER_RETRIES),
        retry_on_error=list(FAILOVER_ERRORS),
        **kwargs,
    )
