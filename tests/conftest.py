"""Import pytest fixtures."""

import fakeredis.aioredis as fakeredis
from psycopg_pool import AsyncConnectionPool
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from unittest.mock import AsyncMock, MagicMock


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
    """
    Create a fake redis client and function for getting that client.

    Handles if the connection decodes response or not.
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
            if db_index == 0:
                return broker_redis
            if db_index == 1:
                return data_redis
            if db_index == 2:
                return lock_redis
            if db_index == 3:
                return logs_redis
            decode = connection_kwargs.get("decode_responses", False)
            return decoded_redis if decode else raw_redis
        return raw_redis

    monkeypatch.setattr(aioredis, "Redis", mock_redis_constructor)

    return {
        "decoded": decoded_redis,
        "raw": raw_redis,
        "broker": broker_redis,
        "data": data_redis,
        "lock": lock_redis,
        "logs": logs_redis,
    }
