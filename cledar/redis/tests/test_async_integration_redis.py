# mypy: disable-error-code=no-untyped-def
import json
from collections.abc import AsyncGenerator
from datetime import datetime
from enum import Enum

import pytest
import pytest_asyncio
from pydantic import BaseModel
from testcontainers.redis import RedisContainer

from cledar.redis import AsyncRedisService, FailedValue, RedisServiceConfig


class UserModel(BaseModel):
    user_id: int
    name: str


class Color(Enum):
    RED = 1
    BLUE = 2


@pytest.fixture(scope="module")
def redis_container():
    """Start a Redis container for testing."""
    with RedisContainer("redis:7.2-alpine") as redis_db:
        yield redis_db


@pytest_asyncio.fixture(scope="function")
async def async_redis_service(
    redis_container: RedisContainer,
) -> AsyncGenerator[AsyncRedisService, None]:
    host = redis_container.get_container_host_ip()
    port = int(redis_container.get_exposed_port(6379))

    config = RedisServiceConfig(redis_host=host, redis_port=port, redis_db=0)
    service = AsyncRedisService(config)
    await service.connect()
    yield service
    await service.close()


@pytest.mark.asyncio
async def test_is_alive(async_redis_service: AsyncRedisService) -> None:
    assert await async_redis_service.is_alive() is True


@pytest.mark.asyncio
async def test_set_and_get_pydantic_model(
    async_redis_service: AsyncRedisService,
) -> None:
    key = "async:user:1"
    model = UserModel(user_id=1, name="Alice")
    assert await async_redis_service.set(key, model) is True
    got = await async_redis_service.get(key, UserModel)
    assert isinstance(got, UserModel)
    assert got == model


@pytest.mark.asyncio
async def test_set_plain_string_and_get_raw(
    async_redis_service: AsyncRedisService,
) -> None:
    key = "async:greeting"
    assert await async_redis_service.set(key, "hello") is True
    assert await async_redis_service.get_raw(key) == "hello"


@pytest.mark.asyncio
async def test_set_with_enum_and_datetime_uses_custom_encoder(
    async_redis_service: AsyncRedisService,
) -> None:
    key = "async:meta"
    now = datetime(2024, 1, 2, 3, 4, 5)
    payload = {"color": Color.RED, "when": now}
    assert await async_redis_service.set(key, payload) is True

    raw = await async_redis_service.get_raw(key)
    data = json.loads(raw)  # type: ignore
    assert data["color"] == "red"
    assert data["when"] == now.isoformat()


@pytest.mark.asyncio
async def test_list_keys(async_redis_service: AsyncRedisService) -> None:
    prefix = "async:listkeys:test:"
    keys = [f"{prefix}{i}" for i in range(3)]
    for k in keys:
        assert await async_redis_service.set(k, {"i": 1}) is True

    listed = await async_redis_service.list_keys(f"{prefix}*")
    for k in keys:
        assert k in listed


@pytest.mark.asyncio
async def test_mget_mixed_results(async_redis_service: AsyncRedisService) -> None:
    ok = UserModel(user_id=2, name="Bob")
    k1 = "async:mget:ok"
    k2 = "async:mget:not_json"
    k3 = "async:mget:bad_validation"
    k4 = "async:mget:none"

    assert await async_redis_service.set(k1, ok) is True
    assert await async_redis_service.set(k2, "{not-json}") is True
    assert await async_redis_service.set(k3, json.dumps({"user_id": 3})) is True

    results = await async_redis_service.mget([k1, k2, k3, k4], UserModel)

    assert isinstance(results[0], UserModel)
    assert isinstance(results[1], FailedValue)
    assert isinstance(results[2], FailedValue)
    assert results[3] is None


@pytest.mark.asyncio
async def test_delete(async_redis_service: AsyncRedisService) -> None:
    key = "async:delete:test"
    assert await async_redis_service.set(key, {"x": 1}) is True
    assert await async_redis_service.delete(key) is True
    assert await async_redis_service.get_raw(key) is None


@pytest.mark.asyncio
async def test_context_manager_pattern(redis_container: RedisContainer) -> None:
    """Test that service can be used with proper async context management."""
    host = redis_container.get_container_host_ip()
    port = int(redis_container.get_exposed_port(6379))

    config = RedisServiceConfig(redis_host=host, redis_port=port, redis_db=0)
    service = AsyncRedisService(config)

    try:
        await service.connect()
        assert await service.is_alive() is True
        await service.set("test:key", "test:value")
        assert await service.get_raw("test:key") == "test:value"
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_concurrent_operations(async_redis_service: AsyncRedisService) -> None:
    """Test multiple concurrent async operations."""
    import asyncio

    async def set_and_get(key: str, value: str) -> str | None:
        await async_redis_service.set(key, value)
        return await async_redis_service.get_raw(key)

    tasks = [set_and_get(f"async:concurrent:{i}", f"value:{i}") for i in range(10)]
    results = await asyncio.gather(*tasks)

    for i, result in enumerate(results):
        assert result == f"value:{i}"
