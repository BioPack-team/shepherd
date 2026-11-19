"""An ARS child is its ARA's response, and Redis is kept to what is hot (#252).

A child's pk is the response id its ARA's query was dispatched with, so the
child and the response are one entry in the data store. The ARS never writes
it: a child's payload is the ARA's own answer, as the ARA would deliver it,
read from Redis while it is hot and from finish_query's archive afterwards --
the same bytes either way. What premerge makes of a child is the merge's
working copy, kept beside it and deleted once the merge has attempted it.

With a cold store configured, the ARS also keeps less durably and lets go of
its Redis copies sooner:

* an ARA child gets no durable copy of its own (the archive is that copy),
  unless its ARA never answered;
* only the newest merged version keeps a durable copy;
* a finished tree's Redis copies are left ``redis_hot_ttl`` to live, and a
  read from a durable copy re-warms Redis for only that long.
"""

import logging
import uuid
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest
import zstandard

import shepherd_utils.db as shepherd_db
from shepherd_utils import response_archive
from shepherd_utils.ars import aras
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
    """Mock pool. ``rows`` maps a message pk to its (status, agent, ref);
    ``data`` maps a pk to a payload still in Postgres. Records the SQL run."""
    state = {"rows": {}, "data": {}, "sql": []}

    async def _execute(sql, params=None):
        state["sql"].append((sql, params))
        cur = MagicMock()
        row = None
        if "SELECT status, agent, ref" in sql:
            row = state["rows"].get(params[0])
        elif "SELECT data IS NOT NULL" in sql:
            row = (params[0] in state["data"],)
        elif "SELECT data" in sql:
            row = (state["data"].get(params[0]),)
        cur.fetchone = AsyncMock(return_value=row)
        return cur

    conn = AsyncMock()
    conn.execute.side_effect = _execute
    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(shepherd_db, "pool", pool)
    return state


def _child(pg, status="D"):
    """An ARA child row: a message under a parent, recorded under its ARA."""
    pk = uuid.uuid4()
    pg["rows"][pk] = (status, "ara-shepherd-aragorn", uuid.uuid4())
    return pk


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


async def _hot(redis_mock, monkeypatch, response_id):
    """Put ``response_id`` in Redis as its ARA's pipeline leaves it: the
    stored response under the id, the logs in the log list, and the query's
    parameters where the lookup finds them."""
    await shepherd_db.save_response(str(response_id), dict(RESPONSE), LOGGER)
    await shepherd_db._append_logs(str(response_id), LOGS)
    monkeypatch.setattr(
        shepherd_db,
        "get_response_query_parameters",
        AsyncMock(return_value=QUERY["parameters"]),
    )


# --- which children keep a durable copy --------------------------------------


@pytest.fixture
def persist(mocker):
    return mocker.patch.object(ars_db, "persist_data_copy", new_callable=AsyncMock)


async def test_a_child_whose_ara_answered_keeps_no_copy_of_its_own(store, persist):
    """The archive finish_query wrote, under the child's own id, is it."""
    await ars_db.persist_child_copy({"id": uuid.uuid4()}, LOGGER)
    persist.assert_not_awaited()


async def test_a_child_whose_ara_never_answered_keeps_its_copy(store, persist):
    """No answer means no archive: whatever the child's entry holds is all
    there is."""
    child = {"id": uuid.uuid4()}
    await ars_db.persist_child_copy(child, LOGGER, have_response=False)
    persist.assert_awaited_once_with(child["id"], LOGGER)


async def test_without_a_store_every_child_keeps_its_copy(no_store, persist):
    child = {"id": uuid.uuid4()}
    await ars_db.persist_child_copy(child, LOGGER)
    persist.assert_awaited_once_with(child["id"], LOGGER)


# --- a child's payload is its ARA's response ---------------------------------


@pytest.fixture
def expired(monkeypatch):
    """Nothing is left in Redis; writes to it are recorded."""
    monkeypatch.setattr(shepherd_db.data_db_client, "get", AsyncMock(return_value=None))
    monkeypatch.setattr(shepherd_db.data_db_client, "exists", AsyncMock(return_value=0))
    rewarm = AsyncMock()
    monkeypatch.setattr(shepherd_db.data_db_client, "set", rewarm)
    return rewarm


async def test_a_hot_child_is_served_as_its_ara_would_deliver_it(
    store, pg, redis_mock, monkeypatch
):
    """The child's entry is the ARA's stored response; what a client gets
    is that response with the query's parameters and logs -- an ordinary
    TRAPI Response -- on every read path."""
    child_pk = _child(pg)
    await _hot(redis_mock, monkeypatch, child_pk)

    data = await ars_db.load_message_data(child_pk, LOGGER)
    assert data["message"] == RESPONSE["message"]
    assert data["parameters"] == {"log_level": "DEBUG"}
    assert data["logs"] == LOGS
    assert data["schema_version"]

    raw = await ars_db.load_message_bytes(child_pk, LOGGER)
    assert orjson.loads(raw) == data
    compressed = await ars_db.load_message_compressed(child_pk, LOGGER)
    assert response_archive.split_frames(compressed) == [raw]
    assert await ars_db.message_has_data(child_pk)


async def test_a_child_reads_the_same_hot_and_cold(store, pg, redis_mock, monkeypatch):
    """From Redis while the response is there, from its archive once it is
    not: the same bytes, so a client cannot tell which it got."""
    child_pk = _child(pg)
    await _hot(redis_mock, monkeypatch, child_pk)
    _archive(child_pk)

    hot = await ars_db.load_message_bytes(child_pk, LOGGER)
    await shepherd_db.data_db_client.flushdb()
    await shepherd_db.logs_db_client.flushdb()
    cold = await ars_db.load_message_bytes(child_pk, LOGGER)

    assert hot is not None and cold == hot
    assert await ars_db.message_has_data(child_pk)


async def test_reading_a_child_never_writes_its_entry(store, pg, expired):
    """The entry is the ARA's, and sealed: a cold read is not re-warmed
    into it."""
    child_pk = _child(pg)
    _archive(child_pk)

    assert await ars_db.load_message_bytes(child_pk, LOGGER) is not None
    assert await ars_db.load_message_data(child_pk, LOGGER) is not None
    assert await ars_db.load_message_compressed(child_pk, LOGGER) is not None
    expired.assert_not_awaited()


@pytest.mark.parametrize("status", ["R", "W"])
async def test_a_child_has_no_payload_until_it_is_terminal(
    store, pg, redis_mock, monkeypatch, status
):
    """Until then the entry holds a response its ARA is still building."""
    child_pk = _child(pg, status=status)
    await _hot(redis_mock, monkeypatch, child_pk)

    assert await ars_db.load_message_data(child_pk, LOGGER) is None
    assert await ars_db.load_message_bytes(child_pk, LOGGER) is None
    assert await ars_db.load_message_compressed(child_pk, LOGGER) is None
    assert not await ars_db.message_has_data(child_pk)


async def test_an_errored_child_still_serves_its_aras_response(
    store, pg, redis_mock, monkeypatch
):
    child_pk = _child(pg, status="E")
    await _hot(redis_mock, monkeypatch, child_pk)
    assert (await ars_db.load_message_data(child_pk, LOGGER))["logs"] == LOGS


async def test_a_child_with_nothing_left_has_no_data(store, pg, expired):
    child_pk = _child(pg)

    assert await ars_db.load_message_data(child_pk, LOGGER) is None
    assert await ars_db.load_message_bytes(child_pk, LOGGER) is None
    assert await ars_db.load_message_compressed(child_pk, LOGGER) is None
    assert not await ars_db.message_has_data(child_pk)


async def test_a_child_whose_ara_never_answered_reads_its_own_copy(store, pg, expired):
    """No archive, but the copy it kept: served as a TRAPI Response too."""
    child_pk = _child(pg, status="E")
    payload_store.write(child_pk, zstandard.compress(b'{"message":{"results":[]}}'))

    data = await ars_db.load_message_data(child_pk, LOGGER)
    assert data["message"] == {"results": []}
    assert data["schema_version"] and "logs" not in data
    assert await ars_db.message_has_data(child_pk)


async def test_without_a_store_a_child_reads_its_copy_from_postgres(
    no_store, pg, expired
):
    child_pk = _child(pg)
    pg["data"][child_pk] = zstandard.compress(b'{"message":{"results":[]}}')

    data = await ars_db.load_message_data(child_pk, LOGGER)
    assert data["message"] == {"results": []}
    assert await ars_db.message_has_data(child_pk)


async def test_a_merged_version_is_not_read_as_a_child(store, pg, redis_mock):
    """It is under the parent too, but it is the ARS's own message: its
    entry is its payload, served as stored."""
    merged_pk = uuid.uuid4()
    pg["rows"][merged_pk] = ("D", aras.MERGE_AGENT, uuid.uuid4())
    await shepherd_db.data_db_client.set(
        str(merged_pk), zstandard.compress(b'{"message":{"merged":true}}')
    )

    assert await ars_db.load_message_bytes(merged_pk, LOGGER) == (
        b'{"message":{"merged":true}}'
    )


async def test_a_failed_row_lookup_reads_the_message_as_it_is_stored(
    store, redis_mock, monkeypatch
):
    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("postgres is down")
    monkeypatch.setattr(shepherd_db, "pool", pool)
    pk = uuid.uuid4()
    await shepherd_db.data_db_client.set(str(pk), zstandard.compress(b'{"a":1}'))

    assert await ars_db.load_message_bytes(pk, LOGGER) == b'{"a":1}'


# --- the merge's working copy ------------------------------------------------


async def test_the_premerged_copy_lives_beside_the_child_not_in_it(redis_mock):
    child_pk = uuid.uuid4()
    key = ars_db.premerged_key(child_pk)
    assert key != str(child_pk) and str(child_pk) in key
    await shepherd_db.data_db_client.set(str(child_pk), b"the ARA's response")
    await shepherd_db.data_db_client.set(key, b"premerged")

    await ars_db.discard_premerged(child_pk, LOGGER)

    assert await shepherd_db.data_db_client.get(key) is None
    assert await shepherd_db.data_db_client.get(str(child_pk)) == b"the ARA's response"


async def test_discarding_a_premerged_copy_never_fails(monkeypatch):
    monkeypatch.setattr(
        shepherd_db.data_db_client,
        "delete",
        AsyncMock(side_effect=ConnectionError("redis is down")),
    )
    await ars_db.discard_premerged(uuid.uuid4(), LOGGER)


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
