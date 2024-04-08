class RedisServiceError(Exception):
    """Base exception for RedisService errors."""


class RedisConnectionError(RedisServiceError):
    """Raised when the Redis connection cannot be established or used."""


class RedisClientNotInitializedError(RedisServiceError):
    """Raised when a Redis operation is attempted without an initialized client."""


class RedisSerializationError(RedisServiceError):
    """Raised when serialization of a value fails before sending to Redis."""


class RedisDeserializationError(RedisServiceError):
    """Raised when deserialization of a value fetched from Redis fails."""


class RedisOperationError(RedisServiceError):
    """Raised for generic Redis operation errors (e.g., command failures)."""
