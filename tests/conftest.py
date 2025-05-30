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

    def mock_redis_constructor(*args, **kwargs):
        if kwargs.get("connection_pool", None):
            decode = kwargs["connection_pool"].connection_kwargs.get("decode_responses", False)
            return decoded_redis if decode else raw_redis
        return raw_redis

    monkeypatch.setattr(aioredis, "Redis", mock_redis_constructor)

    return {
        "decoded": decoded_redis,
        "raw": raw_redis,
    }
