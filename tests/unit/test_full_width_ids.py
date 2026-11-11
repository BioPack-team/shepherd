"""Query, response and callback ids must be wide enough not to collide (#205).

They all key the same data store for ``redis_ttl``. At 8 hex characters (32
bits) a collision becomes likely at Shepherd's volumes, and one silently
overwrote another query's stored query, response or callback.
"""

import logging
import uuid
from unittest.mock import AsyncMock

from shepherd_utils.db import new_id

logger = logging.getLogger(__name__)


def _is_full_uuid(value: str) -> bool:
    return str(uuid.UUID(value)) == value


def test_new_id_is_a_full_random_uuid():
    ids = {new_id() for _ in range(1000)}
    assert len(ids) == 1000
    assert all(_is_full_uuid(i) and uuid.UUID(i).version == 4 for i in ids)


async def test_intake_issues_full_width_query_and_response_ids(monkeypatch):
    from shepherd_server import base_routes

    monkeypatch.setattr(base_routes, "add_query", AsyncMock())
    monkeypatch.setattr(base_routes, "add_task", AsyncMock())
    query = {"message": {"query_graph": {"nodes": {"n0": {}}}}}

    query_id, response_id, _ = await base_routes.run_query(
        "aragorn", query, "http://callback/1"
    )

    assert _is_full_uuid(query_id) and _is_full_uuid(response_id)
    assert query_id != response_id


async def test_ars_dispatch_issues_full_width_ids(monkeypatch, redis_mock):
    from shepherd_utils.ars import aras
    from workers.ars_fanout import worker as fanout

    add_query = AsyncMock()
    monkeypatch.setattr(fanout.shepherd_db, "add_query", add_query)
    child_pk = uuid.uuid4()

    await fanout.dispatch(
        aras.ARAS[0], child_pk, {"id": uuid.uuid4()}, {"message": {}}, logger
    )

    query_id, response_id = add_query.await_args.args[:2]
    assert _is_full_uuid(query_id) and _is_full_uuid(response_id)


async def test_lookup_callback_ids_are_full_width(monkeypatch):
    from shepherd_utils import lookups as lookup

    registered = []

    async def _add_callback_id(query_id, callback_id, otel, _logger):
        registered.append(callback_id)

    monkeypatch.setattr(lookup, "add_callback_id", _add_callback_id)

    class _Client:
        async def post(self, url, json):
            class _R:
                status_code = 200

            return _R()

    await lookup.run_async_lookup(_Client(), {"message": {}}, "q1", "aragorn", logger)

    assert len(registered) == 1 and _is_full_uuid(registered[0])
