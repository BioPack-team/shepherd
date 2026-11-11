"""Postgres pools sized from a fleet connection budget (#223 C11-3).

Every process held a hand-set pool size, and the fleet's ceiling was the sum
of them: about 320 of compose's 400 connections at one replica each, so
scaling replicas ran Postgres out of connections. A process's pool max is now
its share of one budget, and the saturation alert reads max_connections from
the server instead of a hand-counted threshold.
"""

import logging
import re
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from shepherd_utils import db
from workers.monitor.alerts import Rule

REPO = Path(__file__).resolve().parents[2]


def _config(**overrides):
    values = dict(
        postgres_pool_min_size=2,
        postgres_pool_max_size=10,
        postgres_connection_budget=0,
        postgres_fleet_shares=0,
        postgres_pool_shares=1,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


# --- pool_sizes -------------------------------------------------------------------


def test_without_a_budget_the_explicit_sizes_apply():
    assert db.pool_sizes(_config()) == (2, 10)


def test_a_process_takes_its_share_of_the_budget():
    budget = dict(postgres_connection_budget=360, postgres_fleet_shares=35)
    assert db.pool_sizes(_config(**budget)) == (2, 10)
    assert db.pool_sizes(_config(postgres_pool_shares=3, **budget)) == (2, 30)


def test_scaling_the_fleet_shrinks_every_pool():
    """Doubling the replicas doubles the fleet's shares: each pool halves and
    the fleet stays inside the same budget."""
    one = db.pool_sizes(
        _config(postgres_connection_budget=360, postgres_fleet_shares=35)
    )
    two = db.pool_sizes(
        _config(postgres_connection_budget=360, postgres_fleet_shares=70)
    )
    assert two[1] == one[1] // 2
    assert 70 * two[1] <= 360


def test_the_min_size_never_exceeds_the_share():
    sizes = db.pool_sizes(
        _config(
            postgres_pool_min_size=10,
            postgres_connection_budget=100,
            postgres_fleet_shares=50,
        )
    )
    assert sizes == (2, 2)


def test_a_tiny_share_still_gets_a_connection():
    sizes = db.pool_sizes(
        _config(postgres_connection_budget=10, postgres_fleet_shares=50)
    )
    assert sizes == (1, 1)


@pytest.mark.parametrize(
    "overrides, match",
    [
        (dict(postgres_connection_budget=360), "FLEET_SHARES is not"),
        (
            dict(
                postgres_connection_budget=360,
                postgres_fleet_shares=2,
                postgres_pool_shares=3,
            ),
            "exceeds POSTGRES_FLEET_SHARES",
        ),
    ],
)
def test_a_budget_that_cannot_be_divided_is_refused(overrides, match):
    with pytest.raises(ValueError, match=match):
        db.pool_sizes(_config(**overrides))


# --- the startup check -------------------------------------------------------------


class _Pool:
    def __init__(self, max_connections, reserved):
        self.row = (max_connections, reserved)

    @asynccontextmanager
    async def connection(self, *args):
        yield self

    async def execute(self, sql):
        rows = [self.row]

        class _Cursor:
            async def fetchone(self):
                return rows[0]

        return _Cursor()


async def test_a_budget_over_max_connections_is_logged_loudly(monkeypatch, caplog):
    monkeypatch.setattr(db.settings, "postgres_connection_budget", 500)
    monkeypatch.setattr(db, "pool", _Pool(400, 3))

    with caplog.at_level(logging.INFO, logger="shepherd.db"):
        await db.check_connection_budget()

    (record,) = [r for r in caplog.records if "BUDGET" in r.getMessage()]
    assert record.levelno == logging.ERROR
    assert "397" in record.getMessage()


async def test_a_budget_that_fits_is_reported(monkeypatch, caplog):
    monkeypatch.setattr(db.settings, "postgres_connection_budget", 360)
    monkeypatch.setattr(db, "pool", _Pool(400, 3))

    with caplog.at_level(logging.INFO, logger="shepherd.db"):
        await db.check_connection_budget()

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


# --- compose.yml -------------------------------------------------------------------


def _compose():
    return yaml.safe_load((REPO / "compose.yml").read_text())


def _pg_services(compose):
    """Every service built from this repo except the datastores themselves and
    the build-only base image (#223 C11-5): each one opens a Postgres pool
    (get_tasks / the server run initialize_db)."""
    return {
        name: service
        for name, service in compose["services"].items()
        if isinstance(service.get("build"), dict)
        and name not in ("shepherd_db", "shepherd_broker", "shepherd_base")
    }


def _processes(name):
    """Pool-holding processes per container: the server runs one per uvicorn
    worker."""
    if name != "shepherd_server":
        return 1
    dockerfile = (REPO / "shepherd_server" / "Dockerfile").read_text()
    return int(re.search(r"WEB_CONCURRENCY=(\d+)", dockerfile).group(1))


def test_compose_puts_every_postgres_service_on_the_budget():
    compose = _compose()
    budget = compose["x-postgres-budget"]
    for name, service in _pg_services(compose).items():
        env = service.get("environment") or {}
        for key, value in budget.items():
            assert env.get(key) == value, f"{name} is not on the connection budget"
        # a hand-set max would be ignored under the budget; don't leave one
        # around to mislead
        assert "POSTGRES_POOL_MAX_SIZE" not in env, name


def test_compose_fleet_shares_add_up():
    compose = _compose()
    budget = compose["x-postgres-budget"]
    shares = sum(
        _processes(name)
        * int((service.get("environment") or {}).get("POSTGRES_POOL_SHARES", 1))
        for name, service in _pg_services(compose).items()
    )
    assert budget["POSTGRES_FLEET_SHARES"] == shares, (
        "x-postgres-budget's POSTGRES_FLEET_SHARES must be the sum of every "
        f"DB-connecting process's shares ({shares})"
    )


def test_compose_budget_fits_postgres():
    compose = _compose()
    command = compose["services"]["shepherd_db"]["command"]
    max_connections = int(re.search(r"max_connections=(\d+)", command).group(1))
    budget = compose["x-postgres-budget"]["POSTGRES_CONNECTION_BUDGET"]
    # 3 superuser-reserved slots (Postgres' default)
    assert budget <= max_connections - 3


# --- the saturation alert ---------------------------------------------------------


def _snapshot(used, usable):
    return {"postgres": {"client_connections": used, "usable_connections": usable}}


def test_the_saturation_alert_is_a_share_of_max_connections():
    rule = Rule(
        {
            "name": "sat",
            "type": "threshold",
            "metric": "pg_connection_pct",
            "threshold": 80,
        }
    )
    assert rule.evaluate(_snapshot(300, 397)) is None
    detail = rule.evaluate(_snapshot(330, 397))
    assert "330/397" in detail and "83%" in detail
    # the server's limit, not a hand-counted fleet: a bigger Postgres moves it
    assert rule.evaluate(_snapshot(330, 997)) is None


def test_an_unknown_limit_does_not_fire():
    rule = Rule(
        {
            "name": "sat",
            "type": "threshold",
            "metric": "pg_connection_pct",
            "threshold": 80,
        }
    )
    assert rule.evaluate({"postgres": {"error": "down"}}) is None


def test_the_shipped_rule_uses_the_share():
    rules = yaml.safe_load(
        (REPO / "workers" / "monitor" / "monitor_alerts.yaml").read_text()
    )["rules"]
    (rule,) = [r for r in rules if r["name"] == "pg_connection_saturation"]
    assert rule["metric"] == "pg_connection_pct"
