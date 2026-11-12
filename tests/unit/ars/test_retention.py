"""Keeping the ARS's durable storage bounded (shepherd_utils.ars.retention).

The SQL that picks trees was checked against a real Postgres; these tests pin
the decisions around it: eviction only starts at the high mark, stops once
it has freed enough to reach the low mark, takes whole trees in the order
the candidates query returns, drops the cache entries of cache-backed trees
it evicts, and says how far short it fell when nothing evictable is left.
"""

import json
import logging
import os
import time
import uuid
from unittest.mock import AsyncMock

import pytest

from shepherd_utils.ars import payload_store, retention
from shepherd_utils.config import settings
from workers.ars_watchdog import worker as watchdog

LOGGER = logging.getLogger(__name__)
GB = 10**9


def tree(gb, cached=False):
    return {"root": uuid.uuid4(), "file_bytes": int(gb * GB), "cached": cached}


@pytest.fixture
def volume(tmp_path, monkeypatch, mocker):
    """A 100 GB payload volume whose usage the test sets; candidates are
    served from ``queue`` in order, and dropping a tree removes it."""
    monkeypatch.setattr(settings, "ars_payload_dir", str(tmp_path))
    monkeypatch.setattr(settings, "ars_payload_evict_high_pct", 80.0)
    monkeypatch.setattr(settings, "ars_payload_evict_low_pct", 65.0)
    state = {"used": 0, "queue": [], "monkeypatch": monkeypatch, "owned": set()}
    monkeypatch.setattr(payload_store, "usage", lambda: (state["used"], 100 * GB))

    async def owned(message_ids):
        """Archives that are ARS children's (ids put in state["owned"])."""
        return {i for i in message_ids if i in state["owned"]}

    state["owner_lookup"] = mocker.patch.object(
        retention, "_owned_by_a_tree", side_effect=owned
    )

    async def candidates(limit):
        return state["queue"][:limit]

    async def drop(roots, logger):
        dropped = [t for t in state["queue"] if t["root"] in roots]
        state["queue"] = [t for t in state["queue"] if t["root"] not in roots]
        return sum(t["file_bytes"] for t in dropped)

    mocker.patch.object(retention, "_eviction_candidates", side_effect=candidates)
    state["drop"] = mocker.patch.object(retention, "_drop_payloads", side_effect=drop)
    state["drop_cache"] = mocker.patch.object(
        retention, "_drop_cache_entries", new_callable=AsyncMock
    )
    return state


async def test_below_the_high_mark_nothing_is_evicted(volume):
    volume["used"] = 79 * GB
    volume["queue"] = [tree(5)]
    result = await retention.evict_for_space(LOGGER)
    assert result["trees"] == 0
    volume["drop"].assert_not_called()


async def test_evicts_oldest_first_only_until_the_low_mark(volume):
    volume["used"] = 85 * GB  # 20 GB over the 65% low mark
    first, second, third, fourth = tree(8), tree(8), tree(8), tree(8)
    volume["queue"] = [first, second, third, fourth]

    result = await retention.evict_for_space(LOGGER)

    assert result["trees"] == 3
    assert result["freed_bytes"] == 24 * GB
    assert volume["queue"] == [fourth]  # the newest survives
    volume["drop_cache"].assert_not_called()


async def test_cache_backed_trees_lose_their_cache_entries(volume):
    volume["used"] = 81 * GB
    cached = tree(20, cached=True)
    volume["queue"] = [cached]

    await retention.evict_for_space(LOGGER)

    volume["drop_cache"].assert_awaited_once_with([cached["root"]])


async def test_says_how_far_short_when_nothing_is_left_to_evict(volume):
    # the rest of the volume is retained or still-running trees
    volume["used"] = 90 * GB
    volume["queue"] = [tree(5)]

    result = await retention.evict_for_space(LOGGER)

    assert result["trees"] == 1
    assert result["short_bytes"] == 20 * GB


async def test_nothing_happens_without_a_payload_directory(monkeypatch):
    monkeypatch.setattr(settings, "ars_payload_dir", "")
    assert await retention.evict_for_space(LOGGER) == {
        "trees": 0,
        "archived_responses": 0,
        "freed_bytes": 0,
        "short_bytes": 0,
    }


async def test_a_bucket_is_never_evicted_for_space(volume, monkeypatch):
    """S3 has no capacity: the volume's usage is not even looked at."""
    monkeypatch.setattr(settings, "payload_s3_bucket", "shepherd-payloads")
    volume["used"] = 99 * GB
    volume["queue"] = [tree(30)]

    result = await retention.evict_for_space(LOGGER)

    assert result["trees"] == 0 and "total_bytes" not in result
    volume["drop"].assert_not_called()


# --- the archive of ARA responses --------------------------------------------


def archive(response_bytes, age_days=0.0):
    """An archived response last written ``age_days`` ago."""
    response_id = uuid.uuid4()
    payload_store.write(response_id, b"r" * response_bytes, payload_store.ARA)
    when = time.time() - age_days * 86400
    os.utime(payload_store.path_for(response_id, payload_store.ARA), (when, when))
    return response_id


def archived(response_id):
    return payload_store.exists(response_id, payload_store.ARA)


async def test_archived_responses_past_retention_are_purged(volume):
    old, fresh = archive(10, age_days=31), archive(10, age_days=29)

    assert await retention.purge_expired_archive(30, LOGGER) == 1

    assert not archived(old)
    assert archived(fresh)


async def test_the_archive_purge_leaves_ars_messages_alone(volume):
    message = uuid.uuid4()
    payload_store.write(message, b"merged result")
    when = time.time() - 400 * 86400
    os.utime(payload_store.path_for(message), (when, when))

    assert await retention.purge_expired_archive(30, LOGGER) == 0
    assert payload_store.exists(message)


async def test_the_archive_purge_is_off_at_zero_days_or_without_a_store(
    volume, monkeypatch
):
    old = archive(10, age_days=400)
    assert await retention.purge_expired_archive(0, LOGGER) == 0
    assert archived(old)
    monkeypatch.setattr(settings, "ars_payload_dir", "")
    assert await retention.purge_expired_archive(30, LOGGER) == 0


async def test_under_pressure_archived_responses_go_before_any_tree(volume):
    """85 GB used of 100: 20 GB must go. The archive covers it, oldest
    first, so no tree -- a result a client may come back for -- is touched."""
    volume["used"] = 85 * GB
    volume["queue"] = [tree(30)]
    sizes = {"oldest": 15, "middle": 10, "newest": 10}
    ids = {
        "oldest": archive(sizes["oldest"], age_days=3),
        "middle": archive(sizes["middle"], age_days=2),
        "newest": archive(sizes["newest"], age_days=1),
    }
    real_entries = payload_store.entries

    def entries_in_gb(kind):
        for entry in real_entries(kind):
            yield entry._replace(size=entry.size * GB)

    volume["monkeypatch"].setattr(payload_store, "entries", entries_in_gb)

    result = await retention.evict_for_space(LOGGER)

    assert result["archived_responses"] == 2
    assert result["freed_bytes"] == 25 * GB
    assert result["trees"] == 0
    volume["drop"].assert_not_called()
    assert not archived(ids["oldest"]) and not archived(ids["middle"])
    assert archived(ids["newest"])


async def test_trees_are_evicted_only_for_what_the_archive_cannot_free(volume):
    volume["used"] = 85 * GB
    volume["queue"] = [tree(30), tree(30)]
    only = archive(5)
    real_entries = payload_store.entries

    def entries_in_gb(kind):
        for entry in real_entries(kind):
            yield entry._replace(size=entry.size * GB)

    volume["monkeypatch"].setattr(payload_store, "entries", entries_in_gb)

    result = await retention.evict_for_space(LOGGER)

    assert not archived(only)
    assert result["archived_responses"] == 1
    assert result["trees"] == 1  # 5 GB from the archive, then one 30 GB tree
    assert result["freed_bytes"] == 35 * GB


# --- who owns an archive (#252 stage 3) ---------------------------------------


async def test_the_age_purge_leaves_an_ars_childs_archive_to_its_tree(volume):
    """A child's archive is its tree's: kept for as long as the tree keeps
    its payloads (a retained tree, a live cache entry), however old."""
    childs, direct = archive(10, age_days=400), archive(10, age_days=400)
    volume["owned"].add(str(childs))

    assert await retention.purge_expired_archive(30, LOGGER) == 1

    assert archived(childs)
    assert not archived(direct)


async def test_under_pressure_only_direct_query_archives_are_evicted_by_age(volume):
    """An ARS child's archive goes with its tree, not ahead of it."""
    volume["used"] = 85 * GB
    volume["queue"] = [tree(30)]
    childs = archive(15, age_days=3)
    direct = archive(5, age_days=1)
    volume["owned"].add(str(childs))
    real_entries = payload_store.entries

    def entries_in_gb(kind):
        for entry in real_entries(kind):
            yield entry._replace(size=entry.size * GB)

    volume["monkeypatch"].setattr(payload_store, "entries", entries_in_gb)

    result = await retention.evict_for_space(LOGGER)

    assert archived(childs) and not archived(direct)
    assert result["archived_responses"] == 1
    assert result["trees"] == 1  # 5 GB of direct archives was not enough
    assert result["freed_bytes"] == 35 * GB


async def test_an_owner_lookup_that_fails_deletes_nothing(volume):
    """An archive is never deleted because Postgres could not be asked
    whose it is."""
    old = archive(10, age_days=400)
    volume["owner_lookup"].side_effect = RuntimeError("postgres is down")

    with pytest.raises(RuntimeError):
        await retention.purge_expired_archive(30, LOGGER)

    assert archived(old)


async def test_owners_are_looked_up_in_batches(volume, monkeypatch):
    monkeypatch.setattr(retention, "OWNER_BATCH", 2)
    ids = [archive(1, age_days=31 + n) for n in range(5)]

    assert await retention.purge_expired_archive(30, LOGGER) == 5

    assert [len(c.args[0]) for c in volume["owner_lookup"].await_args_list] == [2, 2, 1]
    assert not any(archived(i) for i in ids)


async def test_ownership_is_an_ars_message_row_with_that_id(monkeypatch):
    """The real lookup: one query, and ids that are not UUIDs are nobody's."""
    from unittest.mock import MagicMock

    import shepherd_utils.db as shepherd_db

    child, direct = uuid.uuid4(), uuid.uuid4()
    seen = {}

    async def _execute(sql, params=None):
        seen["sql"], seen["params"] = sql, params
        cur = MagicMock()
        cur.fetchall = AsyncMock(return_value=[(child,)])
        return cur

    conn = AsyncMock()
    conn.execute.side_effect = _execute
    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(shepherd_db, "pool", pool)

    owned = await retention._owned_by_a_tree([str(child), str(direct), "not-a-uuid"])

    assert owned == {str(child)}
    assert "FROM ars_message WHERE id = ANY" in seen["sql"]
    assert seen["params"] == ([child, direct],)
    assert await retention._owned_by_a_tree(["not-a-uuid"]) == set()


async def test_dropping_a_trees_payloads_takes_its_childrens_archives(
    tmp_path, monkeypatch
):
    """The tree owns them: a child's archive is filed under the child's own
    id and goes with the tree's other payloads."""
    from unittest.mock import MagicMock

    import shepherd_utils.db as shepherd_db

    monkeypatch.setattr(settings, "ars_payload_dir", str(tmp_path))
    root, child, merged = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    payload_store.write(root, b"query")
    payload_store.write(merged, b"merged result")
    payload_store.write(child, b"the ARA's archived response", payload_store.ARA)
    other = uuid.uuid4()
    payload_store.write(other, b"someone else's", payload_store.ARA)

    async def _execute(sql, params=None):
        cur = MagicMock()
        # (id, data_size, data IS NULL) for every stored member of the tree
        cur.fetchall = AsyncMock(
            return_value=[(root, 5, True), (child, 27, True), (merged, 13, True)]
        )
        return cur

    conn = AsyncMock()
    conn.execute.side_effect = _execute
    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(shepherd_db, "pool", pool)
    monkeypatch.setattr(shepherd_db, "lift_statement_timeout", AsyncMock())

    freed = await retention._drop_payloads([root], LOGGER)

    assert freed == 45  # the child's archive is counted with the rest
    assert not payload_store.exists(root) and not payload_store.exists(merged)
    assert not payload_store.exists(child, payload_store.ARA)
    assert payload_store.exists(other, payload_store.ARA)


async def test_a_failing_archive_eviction_still_evicts_trees(volume, mocker):
    volume["used"] = 85 * GB
    volume["queue"] = [tree(30)]
    mocker.patch.object(retention, "_evict_archive", side_effect=OSError("io error"))

    result = await retention.evict_for_space(LOGGER)

    assert result["trees"] == 1


async def test_usage_is_published_for_the_monitor(volume, redis_mock, mocker):
    mocker.patch.object(retention, "retained_bytes", AsyncMock(return_value=3 * GB))
    volume["used"] = 85 * GB
    volume["queue"] = [tree(30)]
    eviction = await retention.evict_for_space(LOGGER)

    await retention.publish_usage(eviction, LOGGER, broker=redis_mock["broker"])

    state = json.loads(await redis_mock["broker"].get(retention.USAGE_KEY))
    assert state["used_bytes"] == 55 * GB  # after this pass's deletes
    assert state["total_bytes"] == 100 * GB
    assert state["retained_bytes"] == 3 * GB


async def test_age_passes_are_off_at_zero_days():
    assert await retention.purge_expired_payloads(0, LOGGER) == 0
    assert await retention.delete_expired_trees(0, LOGGER) == 0


# --- the watchdog loop ------------------------------------------------------


@pytest.fixture
def passes(mocker):
    return {
        "evict": mocker.patch.object(
            retention, "evict_for_space", AsyncMock(return_value={})
        ),
        "publish": mocker.patch.object(retention, "publish_usage", AsyncMock()),
        "purge": mocker.patch.object(
            retention, "purge_expired_payloads", AsyncMock(return_value=0)
        ),
        "delete": mocker.patch.object(
            retention, "delete_expired_trees", AsyncMock(return_value=0)
        ),
    }


async def test_eviction_runs_every_pass_the_age_passes_hourly(passes):
    await watchdog.retention_pass(LOGGER, hourly=False)
    passes["evict"].assert_awaited_once()
    passes["purge"].assert_not_awaited()
    passes["delete"].assert_not_awaited()

    await watchdog.retention_pass(LOGGER, hourly=True)
    passes["purge"].assert_awaited_once_with(settings.ars_data_retention_days, LOGGER)
    passes["delete"].assert_awaited_once_with(settings.ars_row_retention_days, LOGGER)


async def test_one_failing_pass_does_not_skip_the_others(passes):
    passes["evict"].side_effect = RuntimeError("volume gone")
    passes["purge"].side_effect = RuntimeError("pg down")

    await watchdog.retention_pass(LOGGER, hourly=True)

    passes["publish"].assert_awaited_once()
    passes["delete"].assert_awaited_once()
