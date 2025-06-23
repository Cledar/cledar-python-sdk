from datetime import datetime
from enum import Enum
from typing import Optional, Any, Type, TypeVar
import json
import logging
from dataclasses import dataclass
from pydantic import BaseModel, ValidationError
import redis

logger = logging.getLogger("redis_service")


class CustomEncoder(json.JSONEncoder):
    """Custom JSON encoder that can handle Enum objects and datetime objects."""

    def default(self, o: Any) -> Any:
        if isinstance(o, Enum):
            return o.name
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


T = TypeVar("T", bound=BaseModel)


@dataclass
class RedisServiceConfig:
    redis_host: str
    redis_port: int
    redis_db: int = 0
    redis_password: Optional[str] = None


class RedisService:
    def __init__(self, config: RedisServiceConfig):
        self.config = config
        self._client: Optional[redis.Redis] = None
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
        except redis.ConnectionError:
            logger.exception("Failed to initialize Redis client.")
            self._client = None

    def is_alive(self) -> bool:
        if self._client is None:
            return False
        try:
            return bool(self._client.ping())
        except redis.ConnectionError:
            logger.exception("Redis connection error during health check.")
            return False

    def set(self, key: str, value: Any) -> bool:
        if self._client is None:
            logger.error("Redis client not initialized.")
            return False

        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            return bool(self._client.set(key, value))
        except (redis.RedisError, TypeError, ValueError):
            logger.exception("Error setting Redis key.", extra={"key": key})
            return False

    def get(self, key: str, model: Type[T]) -> T | None:
        if self._client is None:
            logger.error("Redis client not initialized.")
            return None

        try:
            value = self._client.get(key)
            if value is None:
                return None

            try:
                # Try to parse as JSON
                return model.model_validate(json.loads(str(value)))
            except json.JSONDecodeError:
                logger.exception("JSON Decode error.", extra={"key": key})
                return None
            except ValidationError:
                logger.exception(
                    "Validation error.", extra={"key": key, "model": model}
                )
                return None

        except redis.RedisError:
            logger.exception("Error getting Redis key.", extra={"key": key})
            return None
