"""Import pytest fixtures."""

# Install a stub `shepherd_utils.otel` into sys.modules before any worker
# module imports it. This avoids two problems at once:
#   1. The real module starts a BatchSpanProcessor that pushes to a Jaeger
#      collector that does not exist during tests, so the OTLP exporter
#      retries on shutdown and stalls pytest by ~30s.
#   2. The real module imports opentelemetry.instrumentation.httpx, which
#      pulls in pkg_resources -- not installed in tox-managed venvs by
#      default, breaking conftest collection in CI.
# Worker modules only need `setup_tracer`; a MagicMock tracer is fine for
# tests because none of them exercise the process_task span path.
import sys  # noqa: E402
import types  # noqa: E402
from unittest.mock import MagicMock  # noqa: E402

_otel_stub = types.ModuleType("shepherd_utils.otel")
_otel_stub.setup_tracer = lambda service_name: MagicMock()
sys.modules["shepherd_utils.otel"] = _otel_stub

import fakeredis.aioredis as fakeredis  # noqa: E402
from psycopg_pool import AsyncConnectionPool  # noqa: E402
import pytest  # noqa: E402
import redis.asyncio as aioredis  # noqa: E402
from unittest.mock import AsyncMock  # noqa: E402


@pytest.fixture
def postgres_mock():
    """
    Factory for creating a mock postgres pool with custom return value.
    """

    def _create_mock(return_value):
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = return_value

        mock_pool = AsyncMock(spec=AsyncConnectionPool)
        mock_pool.connection.return_value.__aenter__.return_value = mock_conn
        mock_pool.connection.return_value.__aexit__.return_value = None

        return mock_conn, mock_pool

    return _create_mock


@pytest.fixture()
def redis_mock(monkeypatch):
    """Replace every Redis client the workers reach for with fakeredis.

    Both ``shepherd_utils.db`` and ``shepherd_utils.broker`` build their
    Redis clients at module import (``data_db_client``, ``logs_db_client``,
    ``broker_client``, ``lock_client``), so a constructor monkeypatch alone
    is too late. We swap the live attributes on those modules in addition
    to patching ``aioredis.Redis`` for any client created later in the test.
    """
    decoded_redis = fakeredis.FakeRedis(decode_responses=True)
    raw_redis = fakeredis.FakeRedis(decode_responses=False)
    broker_redis = fakeredis.FakeRedis(decode_responses=True)
    data_redis = fakeredis.FakeRedis(decode_responses=False)
    lock_redis = fakeredis.FakeRedis(decode_responses=True)
    logs_redis = fakeredis.FakeRedis(decode_responses=False)

    def mock_redis_constructor(*args, **kwargs):
        if kwargs.get("connection_pool", None):
            connection_kwargs = kwargs["connection_pool"].connection_kwargs
            db_index = connection_kwargs.get("db", 0)
            decode = connection_kwargs.get("decode_responses", False)
            if db_index == 0:
                return broker_redis
            if db_index == 1:
                return data_redis
            if db_index == 2:
                return lock_redis
            if db_index == 3:
                return logs_redis
            return decoded_redis if decode else raw_redis
        return raw_redis

    monkeypatch.setattr(aioredis, "Redis", mock_redis_constructor)

    # Replace the eagerly-constructed clients so already-imported worker
    # modules use fakeredis through their existing references.
    import shepherd_utils.db as db_module
    import shepherd_utils.broker as broker_module

    monkeypatch.setattr(db_module, "data_db_client", data_redis)
    monkeypatch.setattr(db_module, "logs_db_client", logs_redis)
    monkeypatch.setattr(broker_module, "broker_client", broker_redis)
    monkeypatch.setattr(broker_module, "lock_client", lock_redis)

    return {
        "decoded": decoded_redis,
        "raw": raw_redis,
        "broker": broker_redis,
        "data": data_redis,
        "lock": lock_redis,
        "logs": logs_redis,
    }
