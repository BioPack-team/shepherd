"""Exposure fixes (#207).

- The monitor's admin routes (one ACKs and deletes queued work) answered
  anyone who could reach the monitor (C10-1).
- The API server's CORS policy echoed any Origin back with credentials
  allowed (C3-12).
- The broker's password was baked into redis.conf, so REDIS_PASSWORD changed
  what clients sent but not what the server expected (C2-1).
"""

import shutil
import socket
import subprocess
import time
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

REPO = Path(__file__).resolve().parents[2]

ADMIN_ROUTES = [
    "/api/admin/cleanup",
    "/api/admin/reconcile_lost_queries",
    "/api/admin/reclaim_dead_consumers",
    "/api/admin/forget_worker?name=x",
]


@pytest.fixture
def monitor(monkeypatch):
    from workers.monitor import janitor, recovery
    from workers.monitor import worker as monitor_worker

    calls = AsyncMock(return_value={})
    monkeypatch.setattr(janitor, "run_once", calls)
    monkeypatch.setattr(janitor, "reclaim_dead_consumers", calls)
    monkeypatch.setattr(recovery, "reconcile_lost_queries", AsyncMock(return_value=[]))
    pipe = AsyncMock()
    pipe.srem = lambda *a: None
    pipe.delete = lambda *a: None

    class _Broker:
        def pipeline(self):
            return pipe

    monkeypatch.setattr("shepherd_utils.broker.broker_client", _Broker())
    transport = httpx.ASGITransport(app=monitor_worker.APP)
    return httpx.AsyncClient(transport=transport, base_url="http://monitor"), calls


@pytest.mark.parametrize("route", ADMIN_ROUTES)
async def test_admin_routes_refuse_without_a_token(monitor, monkeypatch, route):
    from shepherd_utils.config import settings

    client, calls = monitor
    monkeypatch.setattr(settings, "monitor_admin_token", "s3cret")
    response = await client.post(route)
    assert response.status_code == 403
    calls.assert_not_awaited()


@pytest.mark.parametrize("route", ADMIN_ROUTES)
async def test_admin_routes_are_off_when_no_token_is_configured(
    monitor, monkeypatch, route
):
    from shepherd_utils.config import settings

    client, _ = monitor
    monkeypatch.setattr(settings, "monitor_admin_token", "")
    response = await client.post(route, headers={"Authorization": "Bearer "})
    assert response.status_code == 403


@pytest.mark.parametrize("route", ADMIN_ROUTES)
async def test_admin_routes_answer_the_right_token(monitor, monkeypatch, route):
    from shepherd_utils.config import settings

    client, _ = monitor
    monkeypatch.setattr(settings, "monitor_admin_token", "s3cret")
    wrong = await client.post(route, headers={"Authorization": "Bearer nope"})
    right = await client.post(route, headers={"Authorization": "Bearer s3cret"})
    assert wrong.status_code == 403
    assert right.status_code == 200


async def test_read_only_monitor_routes_stay_open(monitor):
    client, _ = monitor
    response = await client.get("/api/health")
    assert response.status_code == 200


# --- CORS (C3-12) -------------------------------------------------------------


async def test_cors_does_not_grant_credentials_to_any_origin():
    from shepherd_server.server import APP

    transport = httpx.ASGITransport(app=APP)
    async with httpx.AsyncClient(transport=transport, base_url="http://shepherd") as c:
        response = await c.options(
            "/aragorn/query",
            headers={
                "Origin": "https://evil.example",
                "Access-Control-Request-Method": "POST",
            },
        )
    assert response.headers.get("access-control-allow-credentials") != "true"
    # still usable cross-origin without credentials
    assert response.headers.get("access-control-allow-origin") == "*"


# --- Redis password (C2-1) ----------------------------------------------------


def test_redis_conf_carries_no_password():
    conf = (REPO / "shepherd_broker" / "redis.conf").read_text()
    assert not any(
        line.split()[0] == "requirepass"
        for line in conf.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.skipif(not shutil.which("redis-server"), reason="redis-server missing")
def test_broker_entrypoint_uses_the_password_from_the_environment(tmp_path):
    """Start redis the way the image does, with a non-default REDIS_PASSWORD,
    and check that password -- and only that one -- is accepted."""
    import redis

    port = _free_port()
    # The image's config, minus what only works inside the container (the
    # /data volume, the log file, persistence, the port): run it locally.
    local_only = {"port", "dir", "logfile", "appendonly", "save", "dbfilename"}
    conf = [
        line
        for line in (REPO / "shepherd_broker" / "redis.conf").read_text().splitlines()
        if not (line.split() and line.split()[0] in local_only)
    ]
    conf += [f"port {port}", f"dir {tmp_path}", "appendonly no", 'save ""']
    (tmp_path / "redis.conf").write_text("\n".join(conf) + "\n")
    entrypoint = REPO / "shepherd_broker" / "entrypoint.sh"
    proc = subprocess.Popen(
        ["sh", str(entrypoint), str(tmp_path / "redis.conf")],
        env={"PATH": "/usr/bin:/bin", "REDIS_PASSWORD": "rotated-pw"},
    )
    try:
        for _ in range(50):
            try:
                if redis.Redis(port=port, password="rotated-pw").ping():
                    break
            except redis.exceptions.ConnectionError:
                time.sleep(0.1)
        assert redis.Redis(port=port, password="rotated-pw").ping()
        with pytest.raises(redis.exceptions.AuthenticationError):
            redis.Redis(port=port, password="supersecretpassword").ping()
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.mark.skipif(not shutil.which("redis-server"), reason="redis-server missing")
@pytest.mark.parametrize(
    "env,expected",
    [
        # redis.conf's own value, sized for a local stack
        ({}, 2 * 1024**3),
        ({"REDIS_MAXMEMORY": ""}, 2 * 1024**3),
        # a deployment's
        ({"REDIS_MAXMEMORY": "25gb"}, 25 * 1024**3),
    ],
)
def test_broker_maxmemory_is_local_by_default_and_set_by_the_environment(
    tmp_path, env, expected
):
    """The config's maxmemory suits a laptop; a deployment gives the broker
    more with REDIS_MAXMEMORY instead of shipping its own redis.conf."""
    import redis

    port = _free_port()
    local_only = {"port", "dir", "logfile", "appendonly", "save", "dbfilename"}
    conf = [
        line
        for line in (REPO / "shepherd_broker" / "redis.conf").read_text().splitlines()
        if not (line.split() and line.split()[0] in local_only)
    ]
    conf += [f"port {port}", f"dir {tmp_path}", "appendonly no", 'save ""']
    (tmp_path / "redis.conf").write_text("\n".join(conf) + "\n")
    proc = subprocess.Popen(
        [
            "sh",
            str(REPO / "shepherd_broker" / "entrypoint.sh"),
            str(tmp_path / "redis.conf"),
        ],
        env={"PATH": "/usr/bin:/bin", "REDIS_PASSWORD": "pw", **env},
    )
    try:
        client = redis.Redis(port=port, password="pw")
        for _ in range(50):
            try:
                if client.ping():
                    break
            except redis.exceptions.ConnectionError:
                time.sleep(0.1)
        assert int(client.config_get("maxmemory")["maxmemory"]) == expected
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_the_default_redis_ttl_outlasts_the_longest_query():
    """The default is sized for a local stack, not for keeping finished work
    (the cold store does that), but it must never be shorter than a query
    can run, or Redis would drop payloads still in use."""
    from shepherd_utils.config import Settings

    defaults = Settings(_env_file=None)
    longest = defaults.ars_timeout_standard_sec + defaults.ars_timeout_merge_sec
    assert defaults.redis_ttl >= 4 * longest
    assert defaults.redis_hot_ttl < defaults.redis_ttl


def test_broker_entrypoint_refuses_to_start_without_a_password():
    entrypoint = REPO / "shepherd_broker" / "entrypoint.sh"
    if not entrypoint.exists():
        pytest.fail("shepherd_broker/entrypoint.sh is missing")
    result = subprocess.run(
        ["sh", str(entrypoint), "/dev/null"],
        env={"PATH": "/usr/bin:/bin"},
        capture_output=True,
        timeout=10,
    )
    assert result.returncode != 0
