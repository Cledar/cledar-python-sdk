import json
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, TypeVar, cast

import redis
import redis.asyncio as aioredis
from pydantic import BaseModel, ValidationError

from .exceptions import (
    RedisConnectionError,
    RedisDeserializationError,
    RedisOperationError,
    RedisSerializationError,
)

logger = logging.getLogger("redis_service")


class CustomEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that can handle Enum objects and datetime objects.
    """

    def default(self, o: Any) -> Any:
        if isinstance(o, Enum):
            return o.name.lower()
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


T = TypeVar("T", bound=BaseModel)


@dataclass
class FailedValue:
    key: str
    error: Exception


@dataclass
class RedisServiceConfig:
    redis_host: str
    redis_port: int
    redis_db: int = 0
    redis_password: str | None = None


class RedisService:
    def __init__(self, config: RedisServiceConfig):
        self.config = config
        self._client: redis.Redis
        self.connect()

    def connect(self) -> None:
        try:
            self._client = redis.Redis(
                host=self.config.redis_host,
                port=self.config.redis_port,
                db=self.config.redis_db,
                password=self.config.redis_password,
                decode_responses=True,
            )
            logger.info(
                "Redis client initialized.",
                extra={
                    "host": self.config.redis_host,
                    "port": self.config.redis_port,
                    "db": self.config.redis_db,
                },
            )
            self._client.ping()
            logger.info(
                "Redis client pinged successfully.",
                extra={"host": self.config.redis_host},
            )
        except redis.ConnectionError as exc:
            logger.exception("Failed to initialize Redis client.")
            raise RedisConnectionError("Could not initialize Redis client") from exc

    def is_alive(self) -> bool:
        try:
            return bool(self._client.ping())
        except redis.ConnectionError:
            logger.exception(
                "Redis connection error during health check. Can't ping Redis host %s",
                self.config.redis_host,
            )
            return False

    def _prepare_for_serialization(self, value: Any) -> Any:
        """
        Recursively process data structures, converting BaseModel instances to
        serializable dicts.
        """
        if isinstance(value, BaseModel):
            return value.model_dump()
        if isinstance(value, list):
            return [self._prepare_for_serialization(item) for item in value]
        if isinstance(value, dict):
            return {k: self._prepare_for_serialization(v) for k, v in value.items()}
        return value

    def set(self, key: str, value: Any) -> bool:
        if not isinstance(key, str):
            raise ValueError(f"Key must be a string, got {type(key)}")
        if value is None:
            logger.debug("Value is none", extra={"key": key})
        try:
            processed_value = self._prepare_for_serialization(value)
            if isinstance(processed_value, (dict, list)):
                try:
                    final_value = json.dumps(processed_value, cls=CustomEncoder)

                except (TypeError, ValueError) as exc:
                    logger.exception(
                        "Serialization error before setting Redis key.",
                        extra={"key": key},
                    )
                    raise RedisSerializationError(
                        "Failed to serialize value for Redis"
                    ) from exc

            else:
                final_value = processed_value
            return bool(self._client.set(key, final_value))

        except redis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"key": key})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except redis.RedisError as exc:
            logger.exception("Error setting Redis key.", extra={"key": key})
            raise RedisOperationError(f"Failed to set key '{key}'") from exc

    def get(self, key: str, model: type[T]) -> T | None:
        if not isinstance(key, str):
            raise ValueError(f"Key must be a string, got {type(key)}")

        try:
            value = self._client.get(key)
            if value is None:
                logger.debug("Value is none", extra={"key": key})
                return None
            try:
                return model.model_validate(json.loads(str(value)))

            except json.JSONDecodeError as exc:
                logger.exception("JSON Decode error.", extra={"key": key})
                raise RedisDeserializationError(
                    f"Failed to decode JSON for key '{key}'"
                ) from exc

            except ValidationError as exc:
                logger.exception(
                    "Validation error.", extra={"key": key, "model": model}
                )
                raise RedisDeserializationError(
                    f"Validation failed for key '{key}' and model '{model.__name__}'"
                ) from exc

        except redis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"key": key})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except redis.RedisError as exc:
            logger.exception("Error getting Redis key.", extra={"key": key})
            raise RedisOperationError(f"Failed to get key '{key}'") from exc

    def get_raw(self, key: str) -> Any | None:
        if not isinstance(key, str):
            raise ValueError(f"Key must be a string, got {type(key)}")

        try:
            value = self._client.get(key)
            if value is None:
                logger.debug("Value is none", extra={"key": key})
            return value

        except redis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"key": key})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except redis.RedisError as exc:
            logger.exception("Error getting Redis key.", extra={"key": key})
            raise RedisOperationError(f"Failed to get key '{key}'") from exc

    def list_keys(self, pattern: str) -> list[str]:
        if not isinstance(pattern, str):
            raise ValueError(f"Pattern must be a string, got {type(pattern)}")

        try:
            keys_result = self._client.keys(pattern)
            return cast(list[str], keys_result)

        except redis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"pattern": pattern})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except redis.RedisError as exc:
            logger.exception("Error listing Redis keys.", extra={"pattern": pattern})
            raise RedisOperationError(
                f"Failed to list keys for pattern '{pattern}'"
            ) from exc

    def mget(self, keys: list[str], model: type[T]) -> list[T | None | FailedValue]:
        if not isinstance(keys, list):
            raise ValueError(f"Keys must be a list, got {type(keys)}")

        if not keys:
            return []

        try:
            values = cast(list[Any], self._client.mget(keys))
            results: list[T | None | FailedValue] = []

            for value, key in zip(values, keys, strict=False):
                if value is None:
                    results.append(None)
                    continue

                try:
                    validated_data = model.model_validate(json.loads(str(value)))
                    results.append(validated_data)

                except json.JSONDecodeError as exc:
                    logger.exception("JSON Decode error.", extra={"key": key})
                    results.append(FailedValue(key=key, error=exc))
                    continue

                except ValidationError as exc:
                    logger.exception(
                        "Validation error.",
                        extra={"key": key, "model": model.__name__},
                    )
                    results.append(FailedValue(key=key, error=exc))
                    continue

            return results

        except redis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"keys": keys})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except redis.RedisError as exc:
            logger.exception("Error getting multiple Redis keys.")
            raise RedisOperationError("Failed to mget keys") from exc

    def delete(self, key: str) -> bool:
        if not isinstance(key, str):
            raise ValueError(f"Key must be a string, got {type(key)}")

        try:
            result = self._client.delete(key)
            logger.info("Key deleted successfully", extra={"key": key})
            return bool(result)

        except redis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"key": key})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except redis.RedisError as exc:
            logger.exception("Error deleting Redis key.", extra={"key": key})
            raise RedisOperationError(f"Failed to delete key '{key}'") from exc


class AsyncRedisService:
    """Asynchronous Redis service with async/await support."""

    def __init__(self, config: RedisServiceConfig):
        self.config = config
        self._client: aioredis.Redis

    async def connect(self) -> None:
        """Establish connection to Redis asynchronously."""
        try:
            self._client = aioredis.Redis(
                host=self.config.redis_host,
                port=self.config.redis_port,
                db=self.config.redis_db,
                password=self.config.redis_password,
                decode_responses=True,
            )
            logger.info(
                "Async Redis client initialized.",
                extra={
                    "host": self.config.redis_host,
                    "port": self.config.redis_port,
                    "db": self.config.redis_db,
                },
            )
            await self._client.ping()
            logger.info(
                "Async Redis client pinged successfully.",
                extra={"host": self.config.redis_host},
            )
        except aioredis.ConnectionError as exc:
            logger.exception("Failed to initialize async Redis client.")
            raise RedisConnectionError("Could not initialize Redis client") from exc

    async def close(self) -> None:
        """Close the Redis connection."""
        await self._client.aclose()
        logger.info("Async Redis client closed.")

    async def is_alive(self) -> bool:
        """Check if Redis connection is alive."""
        try:
            return bool(await self._client.ping())
        except aioredis.ConnectionError:
            logger.exception(
                "Redis connection error during health check. Can't ping Redis host %s",
                self.config.redis_host,
            )
            return False

    def _prepare_for_serialization(self, value: Any) -> Any:
        """
        Recursively process data structures, converting BaseModel instances to
        serializable dicts.
        """
        if isinstance(value, BaseModel):
            return value.model_dump()
        if isinstance(value, list):
            return [self._prepare_for_serialization(item) for item in value]
        if isinstance(value, dict):
            return {k: self._prepare_for_serialization(v) for k, v in value.items()}
        return value

    async def set(self, key: str, value: Any) -> bool:
        """Set a key-value pair in Redis."""
        if not isinstance(key, str):
            raise ValueError(f"Key must be a string, got {type(key)}")
        if value is None:
            logger.debug("Value is none", extra={"key": key})
        try:
            processed_value = self._prepare_for_serialization(value)
            if isinstance(processed_value, (dict, list)):
                try:
                    final_value = json.dumps(processed_value, cls=CustomEncoder)

                except (TypeError, ValueError) as exc:
                    logger.exception(
                        "Serialization error before setting Redis key.",
                        extra={"key": key},
                    )
                    raise RedisSerializationError(
                        "Failed to serialize value for Redis"
                    ) from exc

            else:
                final_value = processed_value
            return bool(await self._client.set(key, final_value))

        except aioredis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"key": key})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except aioredis.RedisError as exc:
            logger.exception("Error setting Redis key.", extra={"key": key})
            raise RedisOperationError(f"Failed to set key '{key}'") from exc

    async def get(self, key: str, model: type[T]) -> T | None:
        """Get a value from Redis and validate it against a Pydantic model."""
        if not isinstance(key, str):
            raise ValueError(f"Key must be a string, got {type(key)}")

        try:
            value = await self._client.get(key)
            if value is None:
                logger.debug("Value is none", extra={"key": key})
                return None
            try:
                return model.model_validate(json.loads(str(value)))

            except json.JSONDecodeError as exc:
                logger.exception("JSON Decode error.", extra={"key": key})
                raise RedisDeserializationError(
                    f"Failed to decode JSON for key '{key}'"
                ) from exc

            except ValidationError as exc:
                logger.exception(
                    "Validation error.", extra={"key": key, "model": model}
                )
                raise RedisDeserializationError(
                    f"Validation failed for key '{key}' and model '{model.__name__}'"
                ) from exc

        except aioredis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"key": key})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except aioredis.RedisError as exc:
            logger.exception("Error getting Redis key.", extra={"key": key})
            raise RedisOperationError(f"Failed to get key '{key}'") from exc

    async def get_raw(self, key: str) -> Any | None:
        """Get a raw value from Redis without deserialization."""
        if not isinstance(key, str):
            raise ValueError(f"Key must be a string, got {type(key)}")

        try:
            value = await self._client.get(key)
            if value is None:
                logger.debug("Value is none", extra={"key": key})
            return value

        except aioredis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"key": key})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except aioredis.RedisError as exc:
            logger.exception("Error getting Redis key.", extra={"key": key})
            raise RedisOperationError(f"Failed to get key '{key}'") from exc

    async def list_keys(self, pattern: str) -> list[str]:
        """List keys matching a pattern."""
        if not isinstance(pattern, str):
            raise ValueError(f"Pattern must be a string, got {type(pattern)}")

        try:
            keys_result = await self._client.keys(pattern)
            return cast(list[str], keys_result)

        except aioredis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"pattern": pattern})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except aioredis.RedisError as exc:
            logger.exception("Error listing Redis keys.", extra={"pattern": pattern})
            raise RedisOperationError(
                f"Failed to list keys for pattern '{pattern}'"
            ) from exc

    async def mget(
        self, keys: list[str], model: type[T]
    ) -> list[T | None | FailedValue]:
        """Get multiple values from Redis."""
        if not isinstance(keys, list):
            raise ValueError(f"Keys must be a list, got {type(keys)}")

        if not keys:
            return []

        try:
            values = cast(list[Any], await self._client.mget(keys))
            results: list[T | None | FailedValue] = []

            for value, key in zip(values, keys, strict=False):
                if value is None:
                    results.append(None)
                    continue

                try:
                    validated_data = model.model_validate(json.loads(str(value)))
                    results.append(validated_data)

                except json.JSONDecodeError as exc:
                    logger.exception("JSON Decode error.", extra={"key": key})
                    results.append(FailedValue(key=key, error=exc))
                    continue

                except ValidationError as exc:
                    logger.exception(
                        "Validation error.",
                        extra={"key": key, "model": model.__name__},
                    )
                    results.append(FailedValue(key=key, error=exc))
                    continue

            return results

        except aioredis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"keys": keys})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except aioredis.RedisError as exc:
            logger.exception("Error getting multiple Redis keys.")
            raise RedisOperationError("Failed to mget keys") from exc

    async def delete(self, key: str) -> bool:
        """Delete a key from Redis."""
        if not isinstance(key, str):
            raise ValueError(f"Key must be a string, got {type(key)}")

        try:
            result = await self._client.delete(key)
            logger.info("Key deleted successfully", extra={"key": key})
            return bool(result)

        except aioredis.ConnectionError as exc:
            logger.exception("Redis connection error.", extra={"key": key})
            raise RedisConnectionError(
                f"Error connecting to Redis host {self.config.redis_host}"
            ) from exc

        except aioredis.RedisError as exc:
            logger.exception("Error deleting Redis key.", extra={"key": key})
            raise RedisOperationError(f"Failed to delete key '{key}'") from exc
