"""Parents must not stall the ARS watchdog, nor be left Running by a failed
fanout (#203).

The watchdog scans the oldest Running rows under a LIMIT and skips parents
(they are exempt). Parents used to be filtered only after the LIMIT, so enough
long-Running parents filled the window and no child was ever timed out again.
And a fanout that failed before creating children left its parent Running
forever, since nothing else moves a parent.
"""

import logging
import os
import uuid
from unittest.mock import AsyncMock

import pytest

import shepherd_utils.ars.db as ars_db
from shepherd_utils import db as shepherd_db
from workers.ars_fanout import worker as fanout

LOGGER = logging.getLogger(__name__)

ARS_TEST_DSN = os.environ.get("SHEPHERD_TEST_PG_DSN")


class _Limiter:
    def release(self):
        pass


# --- the watchdog scan (C8-1) -----------------------------------------------


async def test_running_scan_filters_parents_in_sql(mocker):
    """The parent exclusion is part of the query, so it applies before LIMIT."""
    cursor = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=cursor)
    pool = mocker.MagicMock()
    pool.connection.return_value.__aenter__.return_value = conn
    mocker.patch.object(shepherd_db, "pool", pool)

    await ars_db.get_running_messages(0, min_age_sec=300, limit=10)

    sql = conn.execute.await_args.args[0]
    assert "m.ref IS NOT NULL" in sql
    assert sql.index("m.ref IS NOT NULL") < sql.index("LIMIT")


@pytest.fixture
async def ars_pg(monkeypatch):
    """A real Postgres with the ARS schema, in a throwaway database.

    Set SHEPHERD_TEST_PG_DSN (a superuser DSN) to run it.
    """
    if not ARS_TEST_DSN:
        pytest.skip("SHEPHERD_TEST_PG_DSN not set")
    import psycopg
    from psycopg_pool import AsyncConnectionPool

    name = f"ars_scan_{uuid.uuid4().hex[:8]}"
    async with await psycopg.AsyncConnection.connect(
        ARS_TEST_DSN, autocommit=True
    ) as admin:
        await admin.execute(f"CREATE DATABASE {name}")
    dsn = ARS_TEST_DSN.rsplit("/", 1)[0] + f"/{name}"
    pool = AsyncConnectionPool(dsn, min_size=1, max_size=2, open=False)
    await pool.open()
    async with pool.connection() as conn:
        for ddl in shepherd_db._ars_schema_statements():
            await conn.execute(ddl)
        await conn.commit()
    monkeypatch.setattr(shepherd_db, "pool", pool)
    try:
        yield pool
    finally:
        await pool.close()
        async with await psycopg.AsyncConnection.connect(
            ARS_TEST_DSN, autocommit=True
        ) as admin:
            await admin.execute(f"DROP DATABASE {name}")


async def test_scan_reaches_a_child_behind_old_running_parents(ars_pg):
    child_id = uuid.uuid4()
    async with ars_pg.connection() as conn:
        parents = []
        for _ in range(3):
            pk = uuid.uuid4()
            parents.append(pk)
            await conn.execute(
                "INSERT INTO ars_message (id, status, agent, ts) "
                "VALUES (%s, 'R', 'ars-default-agent', NOW() - INTERVAL '2 hours')",
                (pk,),
            )
        await conn.execute(
            "INSERT INTO ars_message (id, status, agent, ref, ts) "
            "VALUES (%s, 'R', 'ara-shepherd-aragorn', %s, NOW() - INTERVAL '1 hour')",
            (child_id, parents[0]),
        )
        await conn.commit()

    rows = await ars_db.get_running_messages(0, min_age_sec=300, limit=2)

    assert [row["id"] for row in rows] == [child_id]


async def test_the_watchdog_does_not_overwrite_a_child_that_finished(ars_pg, mocker):
    """C8-4 (#214): the sweep read the child as Running, then it reached Done
    before the timeout write. The write must not turn it into E/598."""
    from workers.ars_watchdog import worker as watchdog

    parent_pk, child_pk = uuid.uuid4(), uuid.uuid4()
    async with ars_pg.connection() as conn:
        await conn.execute(
            "INSERT INTO ars_message (id, status, agent, ts) "
            "VALUES (%s, 'R', 'ars-default-agent', NOW() - INTERVAL '1 hour')",
            (parent_pk,),
        )
        await conn.execute(
            "INSERT INTO ars_message (id, status, code, agent, ref, ts) "
            "VALUES (%s, 'D', 200, 'ara-shepherd-aragorn', %s, "
            "NOW() - INTERVAL '1 hour')",
            (child_pk, parent_pk),
        )
        await conn.commit()
    stale = await ars_db.get_message_row(child_pk)
    stale = dict(stale, status="R", agent_name="ara-shepherd-aragorn")
    mocker.patch.object(
        ars_db, "get_running_messages", new_callable=AsyncMock, return_value=[stale]
    )
    completion = mocker.patch.object(
        watchdog.lifecycle, "check_parent_completion", new_callable=AsyncMock
    )

    assert await watchdog.sweep(LOGGER) == 0

    row = await ars_db.get_message_row(child_pk)
    assert (row["status"], row["code"]) == ("D", 200)
    completion.assert_not_awaited()


async def test_the_watchdog_times_out_a_child_still_running(ars_pg):
    from workers.ars_watchdog import worker as watchdog

    parent_pk, child_pk = uuid.uuid4(), uuid.uuid4()
    async with ars_pg.connection() as conn:
        await conn.execute(
            "INSERT INTO ars_message (id, status, agent, ts) "
            "VALUES (%s, 'R', 'ars-default-agent', NOW() - INTERVAL '1 hour')",
            (parent_pk,),
        )
        await conn.execute(
            "INSERT INTO ars_message (id, status, code, agent, ref, ts) "
            "VALUES (%s, 'R', 202, 'ara-shepherd-aragorn', %s, "
            "NOW() - INTERVAL '1 hour')",
            (child_pk, parent_pk),
        )
        await conn.commit()

    assert await watchdog.sweep(LOGGER) == 1

    row = await ars_db.get_message_row(child_pk)
    assert (row["status"], row["code"]) == ("E", 598)


# --- a failed fanout (C8-2) --------------------------------------------------


@pytest.fixture
def failing_fanout(mocker, redis_mock):
    parent_pk = uuid.uuid4()
    parent = {"id": parent_pk, "status": "R", "name": "", "params": {}, "ref": None}
    claimed = dict(parent, status="E", code=500)
    patched = {
        "parent_pk": parent_pk,
        "claimed": claimed,
        "get_message_row": mocker.patch.object(
            ars_db, "get_message_row", new_callable=AsyncMock, return_value=parent
        ),
        "load_message_data": mocker.patch.object(
            ars_db,
            "load_message_data",
            new_callable=AsyncMock,
            side_effect=ConnectionError("redis blip"),
        ),
        "get_children": mocker.patch.object(
            ars_db, "get_children", new_callable=AsyncMock, return_value=[]
        ),
        "claim": mocker.patch.object(
            ars_db,
            "claim_terminal_transition",
            new_callable=AsyncMock,
            return_value=claimed,
        ),
        "clear_subscriptions": mocker.patch.object(
            ars_db, "clear_subscriptions", new_callable=AsyncMock
        ),
        "notify": mocker.patch.object(
            fanout, "notify_subscribers", new_callable=AsyncMock
        ),
        "on_parent_failed": mocker.patch.object(
            fanout.cache, "on_parent_failed", new_callable=AsyncMock
        ),
    }
    mocker.patch.object(fanout, "mark_task_as_complete", new_callable=AsyncMock)
    mocker.patch.object(fanout, "save_logs", new_callable=AsyncMock)
    return patched


def _task(parent_pk):
    return ("1-0", {"parent_pk": str(parent_pk), "query_id": str(parent_pk)})


async def test_failed_fanout_with_no_children_ends_the_parent(failing_fanout):
    env = failing_fanout
    await fanout.process_task(_task(env["parent_pk"]), None, LOGGER, _Limiter())

    env["claim"].assert_awaited_once_with(str(env["parent_pk"]), "E", 500)
    env["notify"].assert_awaited_once_with(env["claimed"], None, LOGGER)
    env["clear_subscriptions"].assert_awaited_once()
    env["on_parent_failed"].assert_awaited_once_with(env["claimed"], LOGGER)


async def test_failed_fanout_with_children_leaves_the_parent_to_them(
    failing_fanout,
):
    env = failing_fanout
    env["get_children"].return_value = [{"id": uuid.uuid4(), "status": "R"}]

    await fanout.process_task(_task(env["parent_pk"]), None, LOGGER, _Limiter())

    env["claim"].assert_not_awaited()


async def test_a_parent_already_ended_elsewhere_is_not_notified_twice(
    failing_fanout,
):
    env = failing_fanout
    env["claim"].return_value = None

    await fanout.process_task(_task(env["parent_pk"]), None, LOGGER, _Limiter())

    env["notify"].assert_not_awaited()
    env["on_parent_failed"].assert_not_awaited()
