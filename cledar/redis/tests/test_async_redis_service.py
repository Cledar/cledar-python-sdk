# mypy: disable-error-code=no-untyped-def
from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from unittest.mock import AsyncMock, patch

import pytest
import redis.asyncio as aioredis
from pydantic import BaseModel

from cledar.redis import AsyncRedisService, FailedValue, RedisServiceConfig


class UserModel(BaseModel):
    user_id: int
    name: str


class Color(Enum):
    RED = 1
    BLUE = 2


@pytest.fixture(name="config")
def fixture_config() -> RedisServiceConfig:
    return RedisServiceConfig(redis_host="localhost", redis_port=6379, redis_db=0)


@pytest.fixture(name="async_redis_client")
def fixture_async_redis_client() -> AsyncMock:
    client = AsyncMock()
    client.ping = AsyncMock(return_value=True)
    client.aclose = AsyncMock()
    return client


@pytest.fixture(name="service")
def fixture_service(
    config: RedisServiceConfig, async_redis_client: AsyncMock
) -> AsyncRedisService:
    with patch("cledar.redis.redis.aioredis.Redis", return_value=async_redis_client):
        service = AsyncRedisService(config)
        service._client = async_redis_client
        return service


@pytest.mark.asyncio
async def test_connect_success_initializes_client(config: RedisServiceConfig) -> None:
    with patch("cledar.redis.redis.aioredis.Redis") as redis_instance:
        mock_client = AsyncMock()
        mock_client.ping = AsyncMock(return_value=True)
        redis_instance.return_value = mock_client

        service = AsyncRedisService(config)
        await service.connect()

        redis_instance.assert_called_once_with(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            password=config.redis_password,
            decode_responses=True,
        )


@pytest.mark.asyncio
async def test_connect_failure_raises_connection_error(
    config: RedisServiceConfig,
) -> None:
    with patch("cledar.redis.redis.aioredis.Redis") as redis_instance:
        mock_client = AsyncMock()
        mock_client.ping = AsyncMock(side_effect=aioredis.ConnectionError())
        redis_instance.return_value = mock_client

        service = AsyncRedisService(config)
        with pytest.raises(Exception) as exc:
            await service.connect()
        assert "Could not initialize Redis client" in str(exc.value)


@pytest.mark.asyncio
async def test_is_alive_true(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.ping.return_value = True
    assert await service.is_alive() is True


@pytest.mark.asyncio
async def test_is_alive_false_on_exception(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.ping.side_effect = aioredis.ConnectionError()
    assert await service.is_alive() is False


@pytest.mark.asyncio
async def test_set_with_pydantic_model_serializes_and_sets(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    model = UserModel(user_id=1, name="Alice")
    async_redis_client.set = AsyncMock(return_value=True)

    result = await service.set("user:1", model)
    assert result is True
    value = async_redis_client.set.call_args.args[1]

    as_dict = json.loads(value)
    assert as_dict == model.model_dump()


@pytest.mark.asyncio
async def test_set_with_dict_enum_datetime_uses_custom_encoder(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    now = datetime(2024, 1, 2, 3, 4, 5)
    payload = {"color": Color.RED, "when": now}
    async_redis_client.set = AsyncMock(return_value=True)

    assert await service.set("meta", payload) is True
    value = async_redis_client.set.call_args.args[1]
    as_dict = json.loads(value)
    assert as_dict["color"] == "red"
    assert as_dict["when"] == now.isoformat()


@pytest.mark.asyncio
async def test_set_serialization_error_raises(service: AsyncRedisService) -> None:
    bad = {"x": {1}}
    with pytest.raises(Exception) as exc:
        await service.set("k", bad)
    assert "Failed to serialize value" in str(exc.value)


@pytest.mark.asyncio
async def test_set_connection_error_maps(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.set = AsyncMock(side_effect=aioredis.ConnectionError("conn"))
    with pytest.raises(Exception) as exc:
        await service.set("k", {"a": 1})
    assert "Error connecting to Redis host" in str(exc.value)


@pytest.mark.asyncio
async def test_set_redis_error_maps(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.set = AsyncMock(side_effect=aioredis.RedisError("oops"))
    with pytest.raises(Exception) as exc:
        await service.set("k", {"a": 1})
    assert "Failed to set key" in str(exc.value)


@pytest.mark.asyncio
async def test_get_returns_none_for_missing(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.get = AsyncMock(return_value=None)
    assert await service.get("missing", UserModel) is None


@pytest.mark.asyncio
async def test_get_success_deserializes_to_model(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    model = UserModel(user_id=2, name="Bob")
    async_redis_client.get = AsyncMock(return_value=json.dumps(model.model_dump()))
    got = await service.get("user:2", UserModel)
    assert isinstance(got, UserModel)
    assert got == model


@pytest.mark.asyncio
async def test_get_json_decode_error_maps(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.get = AsyncMock(return_value="not-json")
    with pytest.raises(Exception) as exc:
        await service.get("k", UserModel)
    assert "Failed to decode JSON" in str(exc.value)


@pytest.mark.asyncio
async def test_get_validation_error_maps(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.get = AsyncMock(return_value=json.dumps({"user_id": 3}))
    with pytest.raises(Exception) as exc:
        await service.get("k", UserModel)
    assert "Validation failed" in str(exc.value)


@pytest.mark.asyncio
async def test_get_connection_error_maps(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.get = AsyncMock(side_effect=aioredis.ConnectionError("down"))
    with pytest.raises(Exception) as exc:
        await service.get("k", UserModel)
    assert "Error connecting to Redis host" in str(exc.value)


@pytest.mark.asyncio
async def test_get_redis_error_maps(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.get = AsyncMock(side_effect=aioredis.RedisError("nope"))
    with pytest.raises(Exception) as exc:
        await service.get("k", UserModel)
    assert "Failed to get key" in str(exc.value)


@pytest.mark.asyncio
async def test_get_raw_returns_value(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.get = AsyncMock(return_value="raw")
    assert await service.get_raw("k") == "raw"


@pytest.mark.asyncio
async def test_get_raw_errors_map(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.get = AsyncMock(side_effect=aioredis.RedisError("err"))
    with pytest.raises(Exception) as exc:
        await service.get_raw("k")
    assert "Failed to get key" in str(exc.value)


@pytest.mark.asyncio
async def test_list_keys_success(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.keys = AsyncMock(return_value=["a", "b"])
    assert await service.list_keys("*") == ["a", "b"]


@pytest.mark.asyncio
async def test_list_keys_connection_error(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.keys = AsyncMock(side_effect=aioredis.ConnectionError("err"))
    with pytest.raises(Exception) as exc:
        await service.list_keys("*")
    assert "Error connecting to Redis host" in str(exc.value)


@pytest.mark.asyncio
async def test_list_keys_redis_error(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.keys = AsyncMock(side_effect=aioredis.RedisError("err"))
    with pytest.raises(Exception) as exc:
        await service.list_keys("*")
    assert "Failed to list keys" in str(exc.value)


@pytest.mark.asyncio
async def test_mget_empty_returns_empty(service: AsyncRedisService) -> None:
    assert await service.mget([], UserModel) == []


@pytest.mark.asyncio
async def test_mget_success_and_failures(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    good = UserModel(user_id=1, name="A").model_dump()
    bad_json = "{not-json}"
    bad_validation = json.dumps({"user_id": 2})
    none_value = None
    async_redis_client.mget = AsyncMock(
        return_value=[
            json.dumps(good),
            bad_json,
            bad_validation,
            none_value,
        ]
    )
    keys = ["k1", "k2", "k3", "k4"]
    results = await service.mget(keys, UserModel)

    assert isinstance(results[0], UserModel)
    assert isinstance(results[1], FailedValue)
    assert results[1].key == "k2"
    assert isinstance(results[2], FailedValue)
    assert results[2].key == "k3"
    assert results[3] is None


@pytest.mark.asyncio
async def test_mget_connection_error_maps(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.mget = AsyncMock(side_effect=aioredis.ConnectionError("down"))
    with pytest.raises(Exception) as exc:
        await service.mget(["a"], UserModel)
    assert "Error connecting to Redis host" in str(exc.value)


@pytest.mark.asyncio
async def test_mget_redis_error_maps(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.mget = AsyncMock(side_effect=aioredis.RedisError("err"))
    with pytest.raises(Exception) as exc:
        await service.mget(["a"], UserModel)
    assert "Failed to mget keys" in str(exc.value)


@pytest.mark.asyncio
async def test_delete_success(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.delete = AsyncMock(return_value=1)
    assert await service.delete("k") is True
    async_redis_client.delete.assert_called_once_with("k")


@pytest.mark.asyncio
async def test_delete_connection_error_maps(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.delete = AsyncMock(side_effect=aioredis.ConnectionError("down"))
    with pytest.raises(Exception) as exc:
        await service.delete("k")
    assert "Error connecting to Redis host" in str(exc.value)


@pytest.mark.asyncio
async def test_delete_redis_error_maps(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.delete = AsyncMock(side_effect=aioredis.RedisError("err"))
    with pytest.raises(Exception) as exc:
        await service.delete("k")
    assert "Failed to delete key" in str(exc.value)


@pytest.mark.asyncio
async def test_set_plain_string_value(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    async_redis_client.set = AsyncMock(return_value=True)
    assert await service.set("greeting", "hello") is True
    value = (
        async_redis_client.set.call_args.args[1]
        if async_redis_client.set.call_args.args
        else async_redis_client.set.call_args[0][1]
    )
    assert value == "hello"


@pytest.mark.asyncio
async def test_type_validation_errors(service: AsyncRedisService) -> None:
    with pytest.raises(ValueError, match="Key must be a string"):
        await service.set(123, "x")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Key must be a string"):
        await service.get(123, UserModel)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Key must be a string"):
        await service.get_raw(123)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Pattern must be a string"):
        await service.list_keys(123)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Keys must be a list"):
        await service.mget("not-a-list", UserModel)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_close_connection(
    service: AsyncRedisService, async_redis_client: AsyncMock
) -> None:
    await service.close()
    async_redis_client.aclose.assert_called_once()
