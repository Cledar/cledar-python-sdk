# mypy: disable-error-code=no-untyped-def
from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import redis
from pydantic import BaseModel

from cledar.redis.redis import (
    CustomEncoder,
    FailedValue,
    RedisService,
    RedisServiceConfig,
)


class UserModel(BaseModel):
    user_id: int
    name: str


class Color(Enum):
    RED = 1
    BLUE = 2


@pytest.fixture(name="config")
def fixture_config() -> RedisServiceConfig:
    return RedisServiceConfig(redis_host="localhost", redis_port=6379, redis_db=0)


@pytest.fixture(name="redis_client")
def fixture_redis_client() -> MagicMock:
    client = MagicMock()
    client.ping.return_value = True
    return client


@pytest.fixture(name="service")
def fixture_service(
    config: RedisServiceConfig, redis_client: MagicMock
) -> RedisService:
    with patch("cledar.redis.redis.redis.Redis", return_value=redis_client):
        return RedisService(config)


def test_connect_success_initializes_client(config: RedisServiceConfig) -> None:
    with patch("cledar.redis.redis.redis.Redis") as redis_instance:
        RedisService(config)
        redis_instance.assert_called_once_with(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            password=config.redis_password,
            decode_responses=True,
        )


def test_connect_failure_raises_connection_error(config: RedisServiceConfig) -> None:
    with patch("cledar.redis.redis.redis.Redis", side_effect=redis.ConnectionError()):
        with pytest.raises(Exception) as exc:
            RedisService(config)
        assert "Could not initialize Redis client" in str(exc.value)


def test_is_alive_true(service: RedisService, redis_client: MagicMock) -> None:
    redis_client.ping.return_value = True
    assert service.is_alive() is True


def test_is_alive_false_on_exception(
    service: RedisService, redis_client: MagicMock
) -> None:
    redis_client.ping.side_effect = redis.ConnectionError()
    assert service.is_alive() is False


def test_set_with_pydantic_model_serializes_and_sets(
    service: RedisService, redis_client: MagicMock
) -> None:
    model = UserModel(user_id=1, name="Alice")
    redis_client.set.return_value = True

    result = service.set("user:1", model)
    assert result is True
    value = redis_client.set.call_args.args[1]

    as_dict = json.loads(value)
    assert as_dict == model.model_dump()


def test_set_with_dict_enum_datetime_uses_custom_encoder(
    service: RedisService, redis_client: MagicMock
) -> None:
    now = datetime(2024, 1, 2, 3, 4, 5)
    payload = {"color": Color.RED, "when": now}
    redis_client.set.return_value = True

    assert service.set("meta", payload) is True
    value = redis_client.set.call_args.args[1]
    as_dict = json.loads(value)
    assert as_dict["color"] == "red"
    assert as_dict["when"] == now.isoformat()


def test_set_serialization_error_raises(service: RedisService) -> None:
    bad = {"x": {1}}
    with pytest.raises(Exception) as exc:
        service.set("k", bad)
    assert "Failed to serialize value" in str(exc.value)


def test_set_connection_error_maps(
    service: RedisService, redis_client: MagicMock
) -> None:
    redis_client.set.side_effect = redis.ConnectionError("conn")
    with pytest.raises(Exception) as exc:
        service.set("k", {"a": 1})
    assert "Error connecting to Redis host" in str(exc.value)


def test_set_redis_error_maps(service: RedisService, redis_client: MagicMock) -> None:
    redis_client.set.side_effect = redis.RedisError("oops")
    with pytest.raises(Exception) as exc:
        service.set("k", {"a": 1})
    assert "Failed to set key" in str(exc.value)


def test_get_returns_none_for_missing(
    service: RedisService, redis_client: MagicMock
) -> None:
    redis_client.get.return_value = None
    assert service.get("missing", UserModel) is None


def test_get_success_deserializes_to_model(
    service: RedisService, redis_client: MagicMock
) -> None:
    model = UserModel(user_id=2, name="Bob")
    redis_client.get.return_value = json.dumps(model.model_dump())
    got = service.get("user:2", UserModel)
    assert isinstance(got, UserModel)
    assert got == model


def test_get_json_decode_error_maps(
    service: RedisService, redis_client: MagicMock
) -> None:
    redis_client.get.return_value = "not-json"
    with pytest.raises(Exception) as exc:
        service.get("k", UserModel)
    assert "Failed to decode JSON" in str(exc.value)


def test_get_validation_error_maps(
    service: RedisService, redis_client: MagicMock
) -> None:
    redis_client.get.return_value = json.dumps({"user_id": 3})
    with pytest.raises(Exception) as exc:
        service.get("k", UserModel)
    assert "Validation failed" in str(exc.value)


def test_get_connection_error_maps(
    service: RedisService, redis_client: MagicMock
) -> None:
    redis_client.get.side_effect = redis.ConnectionError("down")
    with pytest.raises(Exception) as exc:
        service.get("k", UserModel)
    assert "Error connecting to Redis host" in str(exc.value)


def test_get_redis_error_maps(service: RedisService, redis_client: MagicMock) -> None:
    redis_client.get.side_effect = redis.RedisError("nope")
    with pytest.raises(Exception) as exc:
        service.get("k", UserModel)
    assert "Failed to get key" in str(exc.value)


def test_get_raw_returns_value(service: RedisService, redis_client: MagicMock) -> None:
    redis_client.get.return_value = "raw"
    assert service.get_raw("k") == "raw"


def test_get_raw_errors_map(service: RedisService, redis_client: MagicMock) -> None:
    redis_client.get.side_effect = redis.RedisError("err")
    with pytest.raises(Exception) as exc:
        service.get_raw("k")
    assert "Failed to get key" in str(exc.value)


def test_list_keys_success(service: RedisService, redis_client: MagicMock) -> None:
    redis_client.keys.return_value = ["a", "b"]
    assert service.list_keys("*") == ["a", "b"]


def test_list_keys_connection_error(
    service: RedisService, redis_client: MagicMock
) -> None:
    redis_client.keys.side_effect = redis.ConnectionError("err")
    with pytest.raises(Exception) as exc:
        service.list_keys("*")
    assert "Error connecting to Redis host" in str(exc.value)


def test_list_keys_redis_error(service: RedisService, redis_client: MagicMock) -> None:
    redis_client.keys.side_effect = redis.RedisError("err")
    with pytest.raises(Exception) as exc:
        service.list_keys("*")
    assert "Failed to list keys" in str(exc.value)


def test_mget_empty_returns_empty(service: RedisService) -> None:
    assert service.mget([], UserModel) == []


def test_mget_success_and_failures(
    service: RedisService, redis_client: MagicMock
) -> None:
    good = UserModel(user_id=1, name="A").model_dump()
    bad_json = "{not-json}"
    bad_validation = json.dumps({"user_id": 2})
    none_value = None
    redis_client.mget.return_value = [
        json.dumps(good),
        bad_json,
        bad_validation,
        none_value,
    ]
    keys = ["k1", "k2", "k3", "k4"]
    results = service.mget(keys, UserModel)

    assert isinstance(results[0], UserModel)
    assert isinstance(results[1], FailedValue)
    assert results[1].key == "k2"
    assert isinstance(results[2], FailedValue)
    assert results[2].key == "k3"
    assert results[3] is None


def test_mget_connection_error_maps(
    service: RedisService, redis_client: MagicMock
) -> None:
    redis_client.mget.side_effect = redis.ConnectionError("down")
    with pytest.raises(Exception) as exc:
        service.mget(["a"], UserModel)
    assert "Error connecting to Redis host" in str(exc.value)


def test_mget_redis_error_maps(service: RedisService, redis_client: MagicMock) -> None:
    redis_client.mget.side_effect = redis.RedisError("err")
    with pytest.raises(Exception) as exc:
        service.mget(["a"], UserModel)
    assert "Failed to mget keys" in str(exc.value)


def test_delete_success(service: RedisService, redis_client: MagicMock) -> None:
    redis_client.delete.return_value = 1
    assert service.delete("k") is True
    redis_client.delete.assert_called_once_with("k")


def test_delete_connection_error_maps(
    service: RedisService, redis_client: MagicMock
) -> None:
    redis_client.delete.side_effect = redis.ConnectionError("down")
    with pytest.raises(Exception) as exc:
        service.delete("k")
    assert "Error connecting to Redis host" in str(exc.value)


def test_delete_redis_error_maps(
    service: RedisService, redis_client: MagicMock
) -> None:
    redis_client.delete.side_effect = redis.RedisError("err")
    with pytest.raises(Exception) as exc:
        service.delete("k")
    assert "Failed to delete key" in str(exc.value)


def test_custom_encoder_direct_usage() -> None:
    payload: dict[str, Any] = {"c": Color.BLUE, "d": datetime(2025, 1, 1, 0, 0, 0)}
    s = json.dumps(payload, cls=CustomEncoder)
    data = json.loads(s)
    assert data["c"] == "blue"
    assert data["d"] == "2025-01-01T00:00:00"


def test_set_plain_string_value(service: RedisService, redis_client: MagicMock) -> None:
    redis_client.set.return_value = True
    assert service.set("greeting", "hello") is True
    value = (
        redis_client.set.call_args.args[1]
        if redis_client.set.call_args.args
        else redis_client.set.call_args[0][1]
    )
    assert value == "hello"


def test_type_validation_errors(service: RedisService) -> None:
    with pytest.raises(ValueError, match="Key must be a string"):
        service.set(123, "x")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Key must be a string"):
        service.get(123, UserModel)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Key must be a string"):
        service.get_raw(123)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Pattern must be a string"):
        service.list_keys(123)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Keys must be a list"):
        service.mget("not-a-list", UserModel)  # type: ignore[arg-type]
