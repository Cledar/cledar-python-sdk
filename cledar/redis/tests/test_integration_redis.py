# mypy: disable-error-code=no-untyped-def
import json
from datetime import datetime
from enum import Enum

import pytest
from pydantic import BaseModel
from testcontainers.redis import RedisContainer

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


@pytest.fixture(scope="module")
def redis_container():
    """Start a Redis container for testing."""
    with RedisContainer("redis:7.2-alpine") as redis_db:
        yield redis_db


@pytest.fixture(scope="module")
def redis_service(redis_container: RedisContainer) -> RedisService:
    host = redis_container.get_container_host_ip()
    port = int(redis_container.get_exposed_port(6379))

    config = RedisServiceConfig(redis_host=host, redis_port=port, redis_db=0)
    return RedisService(config)


def test_is_alive(redis_service: RedisService) -> None:
    assert redis_service.is_alive() is True


def test_set_and_get_pydantic_model(redis_service: RedisService) -> None:
    key = "user:1"
    model = UserModel(user_id=1, name="Alice")
    assert redis_service.set(key, model) is True
    got = redis_service.get(key, UserModel)
    assert isinstance(got, UserModel)
    assert got == model


def test_set_plain_string_and_get_raw(redis_service: RedisService) -> None:
    key = "greeting"
    assert redis_service.set(key, "hello") is True
    assert redis_service.get_raw(key) == "hello"


def test_set_with_enum_and_datetime_uses_custom_encoder(
    redis_service: RedisService,
) -> None:
    key = "meta"
    now = datetime(2024, 1, 2, 3, 4, 5)
    payload = {"color": Color.RED, "when": now}
    assert redis_service.set(key, payload) is True

    raw = redis_service.get_raw(key)
    data = json.loads(raw)  # type: ignore
    assert data["color"] == "red"
    assert data["when"] == now.isoformat()


def test_list_keys(redis_service: RedisService) -> None:
    prefix = "listkeys:test:"
    keys = [f"{prefix}{i}" for i in range(3)]
    for k in keys:
        assert redis_service.set(k, {"i": 1}) is True

    listed = redis_service.list_keys(f"{prefix}*")
    for k in keys:
        assert k in listed


def test_mget_mixed_results(redis_service: RedisService) -> None:
    ok = UserModel(user_id=2, name="Bob")
    k1 = "mget:ok"
    k2 = "mget:not_json"
    k3 = "mget:bad_validation"
    k4 = "mget:none"

    assert redis_service.set(k1, ok) is True
    assert redis_service.set(k2, "{not-json}") is True
    assert redis_service.set(k3, json.dumps({"user_id": 3})) is True

    results = redis_service.mget([k1, k2, k3, k4], UserModel)

    assert isinstance(results[0], UserModel)
    assert isinstance(results[1], FailedValue)
    assert isinstance(results[2], FailedValue)
    assert results[3] is None


def test_delete(redis_service: RedisService) -> None:
    key = "delete:test"
    assert redis_service.set(key, {"x": 1}) is True
    assert redis_service.delete(key) is True
    assert redis_service.get_raw(key) is None


def test_custom_encoder_direct_usage() -> None:
    payload = {"c": Color.BLUE, "d": datetime(2025, 1, 1, 0, 0, 0)}
    s = json.dumps(payload, cls=CustomEncoder)
    data = json.loads(s)
    assert data["c"] == "blue"
    assert data["d"] == "2025-01-01T00:00:00"
