"""Durable ARS payloads on a filesystem volume (shepherd_utils.ars.payload_store)
and the ars.db read/write paths that use it.

With ``ars_payload_dir`` set, a message's durable copy is a file instead of
``ars_message.data``; reads still fall back to Postgres for copies written
before it was set, and ``data_size`` records the size either way.
"""

import logging
import os
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
import zstandard

import shepherd_utils.db as shepherd_db
from shepherd_utils.ars import db as ars_db
from shepherd_utils.ars import payload_store
from shepherd_utils.config import settings

LOGGER = logging.getLogger(__name__)
PAYLOAD = b'{"message": {"results": []}}'


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "ars_payload_dir", str(tmp_path))
    return tmp_path


@pytest.fixture
def pg(monkeypatch):
    """Mock pool: records SQL; ``row`` is what a SELECT returns."""
    conn = AsyncMock()
    state = {"row": None, "sql": []}

    async def _execute(sql, params=None):
        state["sql"].append((sql, params))
        cur = MagicMock()
        cur.fetchone = AsyncMock(return_value=state["row"])
        return cur

    conn.execute.side_effect = _execute
    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(shepherd_db, "pool", pool)
    return state


@pytest.fixture
def redis_copy(monkeypatch):
    """The hot Redis copy; None means it has expired."""
    holder = {"blob": None}
    monkeypatch.setattr(
        shepherd_db.data_db_client,
        "get",
        AsyncMock(side_effect=lambda *a: holder["blob"]),
    )
    monkeypatch.setattr(shepherd_db.data_db_client, "set", AsyncMock())
    return holder


def test_files_are_written_atomically_under_a_two_char_fanout(store):
    pk = uuid.uuid4()
    assert payload_store.write(pk, b"abc") == 3
    path = payload_store.path_for(pk)
    assert path == os.path.join(str(store), str(pk)[:2], f"{pk}.zst")
    assert payload_store.read(pk) == b"abc"
    # no temporary file left behind
    assert os.listdir(os.path.dirname(path)) == [f"{pk}.zst"]

    payload_store.write(pk, b"replaced")
    assert payload_store.read(pk) == b"replaced"


def test_missing_files_read_as_absent(store):
    pk = uuid.uuid4()
    assert payload_store.read(pk) is None
    assert not payload_store.exists(pk)
    payload_store.delete(pk)  # no error


def test_usage_reports_the_volume(store):
    used, total = payload_store.usage()
    assert 0 <= used <= total and total > 0


def test_disabled_without_a_directory(monkeypatch):
    monkeypatch.setattr(settings, "ars_payload_dir", "")
    assert not payload_store.enabled()


async def test_durable_copy_goes_to_a_file_and_clears_postgres(store, pg, redis_copy):
    pk = uuid.uuid4()
    blob = zstandard.compress(PAYLOAD)
    redis_copy["blob"] = blob

    await ars_db.persist_data_copy(pk, LOGGER)

    assert payload_store.read(pk) == blob
    sql, params = pg["sql"][-1]
    assert "SET data = NULL, data_size = %s" in sql
    assert params == (len(blob), pk)


async def test_without_a_directory_the_copy_stays_in_postgres(
    monkeypatch, pg, redis_copy
):
    monkeypatch.setattr(settings, "ars_payload_dir", "")
    pk = uuid.uuid4()
    blob = zstandard.compress(PAYLOAD)
    redis_copy["blob"] = blob

    await ars_db.persist_data_copy(pk, LOGGER)

    sql, params = pg["sql"][-1]
    assert "SET data = %s, data_size = %s" in sql
    assert params == (blob, len(blob), pk)


async def test_reads_come_from_the_file_once_redis_expires(store, pg, redis_copy):
    pk = uuid.uuid4()
    payload_store.write(pk, zstandard.compress(PAYLOAD))

    assert await ars_db.load_message_bytes(pk, LOGGER) == PAYLOAD
    assert await ars_db.load_message_data(pk, LOGGER) == {"message": {"results": []}}
    assert pg["sql"] == []  # Postgres never consulted


async def test_copies_written_before_the_directory_was_set_stay_readable(
    store, pg, redis_copy
):
    pk = uuid.uuid4()
    pg["row"] = (zstandard.compress(PAYLOAD),)

    assert await ars_db.load_message_bytes(pk, LOGGER) == PAYLOAD
    compressed = await ars_db.load_message_compressed(pk, LOGGER)
    assert zstandard.decompress(compressed) == PAYLOAD


async def test_no_copy_anywhere_is_none(store, pg, redis_copy):
    assert await ars_db.load_message_bytes(uuid.uuid4(), LOGGER) is None


async def test_has_data_sees_the_file(store, pg, redis_copy, monkeypatch):
    monkeypatch.setattr(shepherd_db, "message_exists", AsyncMock(return_value=False))
    pk = uuid.uuid4()
    pg["row"] = (False,)
    assert not await ars_db.message_has_data(pk)
    payload_store.write(pk, b"x")
    assert await ars_db.message_has_data(pk)
