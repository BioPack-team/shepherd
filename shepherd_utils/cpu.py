"""CPU-count detection that respects container (cgroup) CPU limits.

``os.cpu_count()`` reports the host's logical CPUs, not the cgroup CPU quota the
container is actually limited to. On a Kubernetes node with many cores, a pod
limited to 2 CPUs still sees all of them, so sizing a pool off ``os.cpu_count()``
over-provisions. For the process-pool workers here that means far more
concurrent children -- each loading a full (large) message into memory -- than
the pod's memory budget allows, which cgroup-OOM-kills the whole pod.

``available_cpu_count`` reads the actual CPU quota; ``resolve_pool_workers`` maps
it to a pool size and honours an explicit ``POOL_MAX_WORKERS`` override so a
memory-tight deployment can pin concurrency independent of its CPU allocation.
"""

import logging
import math
import os
from typing import Optional

# Each worker runs as its own Deployment, so a single env var per Deployment
# unambiguously tunes that worker's pool -- mirrors how TASK_LIMIT is resolved.
POOL_WORKERS_ENV = "POOL_MAX_WORKERS"


def _cgroup_cpu_quota() -> Optional[float]:
    """CPU quota (in whole CPUs) from the cgroup, or None if unlimited/unknown."""
    # cgroup v2: "/sys/fs/cgroup/cpu.max" holds "<quota> <period>" or "max ...".
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota_s, period_s = f.read().split()[:2]
        if quota_s != "max":
            quota, period = int(quota_s), int(period_s)
            if quota > 0 and period > 0:
                return quota / period
    except (OSError, ValueError):
        pass
    # cgroup v1: separate quota/period files; quota == -1 means unlimited.
    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f:
            quota = int(f.read())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f:
            period = int(f.read())
        if quota > 0 and period > 0:
            return quota / period
    except (OSError, ValueError):
        pass
    return None


def available_cpu_count() -> int:
    """Best estimate of CPUs available to this process, cgroup-limit aware.

    Prefers the cgroup CPU quota (the Kubernetes CPU *limit*), then the
    scheduler affinity mask, then ``os.cpu_count()``. Always at least 1. A
    fractional quota is rounded up so e.g. 1.5 CPUs still yields 2.
    """
    quota = _cgroup_cpu_quota()
    if quota is not None:
        return max(1, math.ceil(quota))
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except AttributeError:  # not available on every platform
        return max(1, os.cpu_count() or 1)


def resolve_pool_workers(
    task_limit: int, logger: Optional[logging.Logger] = None
) -> int:
    """Resolve a process-pool size for a CPU-bound worker.

    Uses the ``POOL_MAX_WORKERS`` env override when set, otherwise the
    cgroup-aware CPU count. The result is clamped to ``[1, task_limit]`` -- more
    pool workers than in-flight tasks would just sit idle.
    """
    raw = os.environ.get(POOL_WORKERS_ENV)
    if raw is not None:
        try:
            value = int(raw)
            if value < 1:
                raise ValueError
            return min(value, task_limit)
        except ValueError:
            if logger is not None:
                logger.warning(
                    f"Ignoring invalid {POOL_WORKERS_ENV}={raw!r}; "
                    "falling back to detected CPU count."
                )
    return max(1, min(available_cpu_count(), task_limit))
