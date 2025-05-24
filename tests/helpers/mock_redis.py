"""Fake Redis Mocker."""

import fakeredis.aioredis as fakeredis


async def redis_mock():
    """
    Create a fake redis client and function for getting that client.

    The mock_redis_constructor is important because Shepherd creates a new
    redis client (from the pool) for each operation, but when using fake redis,
    each fake redis is its own instance and doesn't have access to other fake redis
    instances. So the function is so that the same fake redis instance is used for
    all operations during testing.
    """
    redis = await fakeredis.FakeRedis(decode_responses=True)

    async def mock_redis_constructor(*args, **kwargs):
        return redis

    return redis, mock_redis_constructor
