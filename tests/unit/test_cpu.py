"""Tests for cgroup-aware CPU/pool-size resolution.

The process-pool workers used to size their pools off ``os.cpu_count()``, which
reports the whole node's cores rather than the pod's cgroup CPU limit. On a big
node that over-provisioned the pool, running many large-message children at once
and OOM-killing the pod. These cover the cgroup-aware replacement.
"""

import shepherd_utils.cpu as cpu
from shepherd_utils.cpu import POOL_WORKERS_ENV, resolve_pool_workers


def test_available_cpu_count_uses_cgroup_v2_quota(monkeypatch):
    # cgroup v2 "cpu.max": "200000 100000" -> 2.0 CPUs.
    monkeypatch.setattr(cpu, "_cgroup_cpu_quota", lambda: 2.0)
    assert cpu.available_cpu_count() == 2


def test_available_cpu_count_rounds_fractional_quota_up(monkeypatch):
    monkeypatch.setattr(cpu, "_cgroup_cpu_quota", lambda: 1.5)
    assert cpu.available_cpu_count() == 2


def test_available_cpu_count_falls_back_when_unlimited(monkeypatch):
    monkeypatch.setattr(cpu, "_cgroup_cpu_quota", lambda: None)
    monkeypatch.setattr(cpu.os, "sched_getaffinity", lambda pid: {0, 1, 2, 3})
    assert cpu.available_cpu_count() == 4


def test_resolve_pool_workers_clamps_to_task_limit(monkeypatch):
    monkeypatch.delenv(POOL_WORKERS_ENV, raising=False)
    # Detected 8 cores but task_limit is 4 -> 4 (extra workers would idle).
    monkeypatch.setattr(cpu, "available_cpu_count", lambda: 8)
    assert resolve_pool_workers(4) == 4
    # Detected 2 cores, task_limit 10 -> 2 (the real CPU allocation wins).
    monkeypatch.setattr(cpu, "available_cpu_count", lambda: 2)
    assert resolve_pool_workers(10) == 2


def test_resolve_pool_workers_env_override(monkeypatch):
    monkeypatch.setenv(POOL_WORKERS_ENV, "1")
    # Even on a big detected count, the override pins the pool to 1.
    monkeypatch.setattr(cpu, "available_cpu_count", lambda: 16)
    assert resolve_pool_workers(10) == 1


def test_resolve_pool_workers_override_clamped_to_task_limit(monkeypatch):
    monkeypatch.setenv(POOL_WORKERS_ENV, "20")
    assert resolve_pool_workers(10) == 10


def test_resolve_pool_workers_invalid_override_falls_back(monkeypatch):
    monkeypatch.setenv(POOL_WORKERS_ENV, "not-a-number")
    monkeypatch.setattr(cpu, "available_cpu_count", lambda: 3)
    assert resolve_pool_workers(10) == 3


def test_resolve_pool_workers_never_below_one(monkeypatch):
    monkeypatch.delenv(POOL_WORKERS_ENV, raising=False)
    monkeypatch.setattr(cpu, "available_cpu_count", lambda: 1)
    assert resolve_pool_workers(10) == 1
