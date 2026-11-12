"""Keeping Redis to what is hot (#252, stage 2).

With a cold store configured, the ARS keeps less durably and lets go of its
Redis copies sooner:

* an ARA child gets no durable copy of its own -- it points at the archive
  finish_query wrote for its ARA's response, and is served from it once its
  Redis copy has expired;
* only the newest merged version keeps a durable copy;
* a finished tree's Redis copies are left ``redis_hot_ttl`` to live, and a
  read from a durable copy re-warms Redis for only that long.

Without a store, none of it applies.
"""

import logging
import uuid
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest
import zstandard

import shepherd_utils.db as shepherd_db
from shepherd_utils import response_archive
from shepherd_utils.ars import db as ars_db
from shepherd_utils.ars import lifecycle, payload_store
from shepherd_utils.config import settings
from workers.ars_merge import worker as merge_worker

LOGGER = logging.getLogger(__name__)
RESPONSE = {"message": {"results": [{"analyses": []}]}}
QUERY = {"message": {"query_graph": {}}, "parameters": {"log_level": "DEBUG"}}
LOGS = [{"timestamp": "2026-01-01T00:00:00Z", "level": "INFO", "message": "hi"}]


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "ars_payload_dir", str(tmp_path))
    monkeypatch.setattr(settings, "payload_s3_bucket", "")
    monkeypatch.setattr(settings, "redis_ttl", 259200)
    monkeypatch.setattr(settings, "redis_hot_ttl", 1800)
    return tmp_path


@pytest.fixture
def no_store(monkeypatch):
    monkeypatch.setattr(settings, "ars_payload_dir", "")
    monkeypatch.setattr(settings, "payload_s3_bucket", "")


@pytest.fixture
def pg(monkeypatch):
    """Mock pool. ``pointers`` maps a message pk to its ara_response_id;
    no message has a payload left in Postgres. Records the SQL run."""
    state = {"pointers": {}, "sql": []}

    async def _execute(sql, params=None):
        state["sql"].append((sql, params))
        cur = MagicMock()
        row = None
        if "SELECT ara_response_id" in sql:
            row = (state["pointers"].get(params[0]),)
        elif "SELECT data" in sql:
            row = (None,)
        cur.fetchone = AsyncMock(return_value=row)
        return cur

    conn = AsyncMock()
    conn.execute.side_effect = _execute
    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(shepherd_db, "pool", pool)
    return state


def _archive(response_id):
    """Write the archive finish_query would have for ``response_id``."""
    blob = (
        zstandard.compress(orjson.dumps(RESPONSE))
        + zstandard.compress(orjson.dumps(QUERY))
        + zstandard.compress(
            orjson.dumps({"status": "OK", "description": None, "logs": LOGS})
        )
    )
    payload_store.write(response_id, blob, payload_store.ARA)


# --- which children keep a durable copy --------------------------------------


@pytest.fixture
def persist(mocker):
    return mocker.patch.object(ars_db, "persist_data_copy", new_callable=AsyncMock)


def _child(pointer=True):
    return {"id": uuid.uuid4(), "ara_response_id": uuid.uuid4() if pointer else None}


async def test_a_child_whose_ara_answered_keeps_no_copy_of_its_own(store, persist):
    await ars_db.persist_child_copy(_child(), LOGGER)
    persist.assert_not_awaited()


async def test_a_child_with_no_answer_to_point_at_keeps_its_copy(store, persist):
    """Its ARA's response never arrived (or was lost): the archive has
    nothing for it, so the child's own payload is all there is."""
    child = _child()
    await ars_db.persist_child_copy(child, LOGGER, have_response=False)
    persist.assert_awaited_once_with(child["id"], LOGGER)


async def test_a_child_from_before_the_pointer_keeps_its_copy(store, persist):
    child = _child(pointer=False)
    await ars_db.persist_child_copy(child, LOGGER)
    persist.assert_awaited_once_with(child["id"], LOGGER)


async def test_without_a_store_every_child_keeps_its_copy(no_store, persist):
    child = _child()
    await ars_db.persist_child_copy(child, LOGGER)
    persist.assert_awaited_once_with(child["id"], LOGGER)


# --- a child read after its Redis copy has expired ---------------------------


@pytest.fixture
def expired(monkeypatch):
    """Nothing is left in Redis; writes to it are recorded."""
    monkeypatch.setattr(shepherd_db.data_db_client, "get", AsyncMock(return_value=None))
    monkeypatch.setattr(shepherd_db.data_db_client, "exists", AsyncMock(return_value=0))
    rewarm = AsyncMock()
    monkeypatch.setattr(shepherd_db.data_db_client, "set", rewarm)
    return rewarm


async def test_a_child_is_served_from_its_aras_archived_response(store, pg, expired):
    """What comes back is the ARA's own answer as it would have delivered
    it -- its response with the query's parameters and logs -- as an
    ordinary TRAPI Response, on every read path."""
    child_pk, response_id = uuid.uuid4(), uuid.uuid4()
    pg["pointers"][child_pk] = response_id
    _archive(response_id)

    data = await ars_db.load_message_data(child_pk, LOGGER)
    assert data["message"] == RESPONSE["message"]
    assert data["parameters"] == {"log_level": "DEBUG"}
    assert data["logs"] == LOGS
    assert data["schema_version"]

    raw = await ars_db.load_message_bytes(child_pk, LOGGER)
    assert orjson.loads(raw) == data
    compressed = await ars_db.load_message_compressed(child_pk, LOGGER)
    # ?compress hands clients one zstd frame of the response, not the archive
    assert response_archive.split_frames(compressed) == [raw]
    assert await ars_db.message_has_data(child_pk)
    expired.assert_not_awaited()  # not re-warmed: it is only being looked at


async def test_a_child_whose_archive_is_gone_has_no_data(store, pg, expired):
    child_pk = uuid.uuid4()
    pg["pointers"][child_pk] = uuid.uuid4()

    assert await ars_db.load_message_data(child_pk, LOGGER) is None
    assert await ars_db.load_message_bytes(child_pk, LOGGER) is None
    assert await ars_db.load_message_compressed(child_pk, LOGGER) is None
    assert not await ars_db.message_has_data(child_pk)


async def test_a_message_that_points_nowhere_has_no_data(store, pg, expired):
    """A superseded merged version, say: no durable copy and no pointer."""
    assert await ars_db.load_message_bytes(uuid.uuid4(), LOGGER) is None
    assert not await ars_db.message_has_data(uuid.uuid4())


async def test_a_childs_own_durable_copy_wins_over_the_archive(store, pg, expired):
    child_pk, response_id = uuid.uuid4(), uuid.uuid4()
    pg["pointers"][child_pk] = response_id
    _archive(response_id)
    payload_store.write(child_pk, zstandard.compress(b'{"own":"copy"}'))

    assert await ars_db.load_message_bytes(child_pk, LOGGER) == b'{"own":"copy"}'


async def test_without_a_store_the_archive_is_never_consulted(no_store, pg, expired):
    child_pk = uuid.uuid4()
    pg["pointers"][child_pk] = uuid.uuid4()

    assert await ars_db.load_message_bytes(child_pk, LOGGER) is None
    assert not any("ara_response_id" in sql for sql, _ in pg["sql"])


# --- how long a re-warmed payload stays in Redis -----------------------------


async def test_a_cold_read_rewarms_redis_for_the_hot_window_only(store, pg, expired):
    pk = uuid.uuid4()
    blob = zstandard.compress(b'{"message":{}}')
    payload_store.write(pk, blob)

    await ars_db.load_message_bytes(pk, LOGGER)
    await ars_db.load_message_compressed(pk, LOGGER)
    await ars_db.load_message_data(pk, LOGGER)

    assert expired.await_count == 3
    for call in expired.await_args_list:
        assert call.args == (str(pk), blob)
        assert call.kwargs == {"ex": 1800}


async def test_the_hot_window_never_exceeds_redis_ttl(store, monkeypatch):
    monkeypatch.setattr(settings, "redis_ttl", 600)
    assert ars_db.rewarm_ttl() == 600


async def test_without_a_store_a_rewarm_lasts_redis_ttl(no_store, monkeypatch):
    monkeypatch.setattr(settings, "redis_ttl", 259200)
    assert ars_db.rewarm_ttl() == 259200


# --- letting go of a finished tree -------------------------------------------


async def test_a_finished_trees_redis_copies_get_the_hot_ttl(store, redis_mock):
    ids = [uuid.uuid4(), uuid.uuid4()]
    for pk in ids:
        await shepherd_db.data_db_client.set(str(pk), b"payload", ex=259200)

    await ars_db.expire_hot_copies(ids + [uuid.uuid4()], LOGGER)  # one has no copy

    for pk in ids:
        assert 0 < await shepherd_db.data_db_client.ttl(str(pk)) <= 1800


async def test_without_a_store_redis_copies_keep_their_ttl(no_store, redis_mock):
    pk = uuid.uuid4()
    await shepherd_db.data_db_client.set(str(pk), b"payload", ex=259200)

    await ars_db.expire_hot_copies([pk], LOGGER)

    assert await shepherd_db.data_db_client.ttl(str(pk)) > 1800


async def test_a_redis_error_expiring_copies_never_escapes(store, monkeypatch):
    monkeypatch.setattr(
        shepherd_db.data_db_client,
        "pipeline",
        MagicMock(side_effect=ConnectionError("redis is down")),
    )
    await ars_db.expire_hot_copies([uuid.uuid4()], LOGGER)


async def test_completing_a_parent_releases_the_whole_tree(store, mocker):
    parent_pk, merged_pk = uuid.uuid4(), uuid.uuid4()
    children = [
        {
            "id": uuid.uuid4(),
            "status": "D",
            "agent_name": agent,
            "code": 200,
            "result_count": 5,
        }
        for agent in ("ara-aragorn", "ars-ars-agent")
    ]
    parent = {
        "id": parent_pk,
        "status": "R",
        "code": 202,
        "merged_version": merged_pk,
        "merged_versions_list": [[str(merged_pk), "ara-aragorn"]],
    }

    def _patch(target, name, **kwargs):
        return mocker.patch.object(target, name, new_callable=AsyncMock, **kwargs)

    _patch(ars_db, "get_message_row", return_value=parent)
    _patch(ars_db, "get_children", return_value=children)
    _patch(
        ars_db,
        "claim_terminal_transition",
        return_value={**parent, "status": "D", "code": 200},
    )
    _patch(ars_db, "clear_subscriptions")
    _patch(ars_db, "persist_data_copy")
    _patch(lifecycle, "notify_subscribers")
    _patch(lifecycle.cache, "on_parent_complete")
    expire = _patch(ars_db, "expire_hot_copies")

    await lifecycle.check_parent_completion(parent_pk, LOGGER)

    (released,) = [call.args[0] for call in expire.await_args_list]
    assert set(released) == {parent_pk, merged_pk, *(c["id"] for c in children)}


async def test_a_parent_that_is_not_complete_releases_nothing(store, mocker):
    parent_pk = uuid.uuid4()
    mocker.patch.object(
        ars_db,
        "get_message_row",
        new_callable=AsyncMock,
        return_value={"id": parent_pk, "status": "R", "code": 202},
    )
    mocker.patch.object(
        ars_db,
        "get_children",
        new_callable=AsyncMock,
        return_value=[
            {
                "id": uuid.uuid4(),
                "status": "R",
                "agent_name": "ara-aragorn",
                "code": 202,
                "result_count": None,
            }
        ],
    )
    expire = mocker.patch.object(ars_db, "expire_hot_copies", new_callable=AsyncMock)

    await lifecycle.check_parent_completion(parent_pk, LOGGER)

    expire.assert_not_awaited()


# --- only the newest merged version is kept ----------------------------------


async def test_dropping_a_durable_copy_removes_the_file_and_its_record(store, pg):
    pk = uuid.uuid4()
    payload_store.write(pk, b"superseded merge")

    await ars_db.drop_durable_copy(pk, LOGGER)

    assert not payload_store.exists(pk)
    sql, params = pg["sql"][-1]
    assert "SET data = NULL, data_size = NULL" in sql
    assert params == (pk,)


async def test_a_failure_dropping_a_copy_never_escapes(store, monkeypatch):
    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("postgres is down")
    monkeypatch.setattr(shepherd_db, "pool", pool)
    await ars_db.drop_durable_copy(uuid.uuid4(), LOGGER)


@pytest.fixture
def merge(mocker, redis_mock):
    """merge_one's collaborators, for a parent that already has a merged
    version. ``outcome`` is what the fold and post-process return."""
    parent_pk, child_pk, previous_pk = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    rows = {
        str(parent_pk): {
            "id": parent_pk,
            "status": "R",
            "merged_version": previous_pk,
            "merged_versions_list": [[str(previous_pk), "ara-shepherd-arax"]],
            "params": {},
        },
        str(child_pk): {"id": child_pk, "agent": "ara-shepherd-aragorn"},
    }
    outcome = {"status": "D", "code": 200, "result_count": 1, "stats": {}}

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    def _update(pk, **kw):
        row = rows.setdefault(str(pk), {"id": pk})
        row.update(kw)
        return dict(row)

    _patch("get_message_row", side_effect=lambda pk: rows.get(str(pk)))
    _patch("create_message", return_value={"id": uuid.uuid4()})
    _patch("update_message", side_effect=_update)
    _patch("load_message_data", return_value={})
    mocker.patch.object(merge_worker, "notify_subscribers", new_callable=AsyncMock)
    mocker.patch.object(
        merge_worker.lifecycle, "check_parent_completion", new_callable=AsyncMock
    )
    mocker.patch.object(
        merge_worker, "_run_merge_in_pool", new_callable=AsyncMock, return_value=outcome
    )
    return {
        "parent_pk": parent_pk,
        "child_pk": child_pk,
        "previous_pk": previous_pk,
        "outcome": outcome,
        "persist": _patch("persist_data_copy"),
        "drop": _patch("drop_durable_copy"),
    }


async def test_a_new_merged_version_supersedes_the_last_ones_durable_copy(store, merge):
    await merge_worker.merge_one(merge["parent_pk"], merge["child_pk"], LOGGER)

    merge["persist"].assert_awaited_once()  # the new version is kept ...
    merge["drop"].assert_awaited_once_with(merge["previous_pk"], LOGGER)  # ... alone


async def test_a_merge_that_did_not_finish_cleanly_supersedes_nothing(store, merge):
    merge["outcome"].update(status="E", code=444)

    await merge_worker.merge_one(merge["parent_pk"], merge["child_pk"], LOGGER)

    merge["drop"].assert_not_awaited()


async def test_without_a_store_every_merged_version_is_kept(no_store, merge):
    await merge_worker.merge_one(merge["parent_pk"], merge["child_pk"], LOGGER)

    merge["persist"].assert_awaited_once()
    merge["drop"].assert_not_awaited()
