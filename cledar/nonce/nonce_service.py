"""Simple nonce service for preventing duplicate request processing.

Uses Redis with TTL for automatic cleanup.
"""

from cledar.redis.redis import RedisService


class NonceService:
    """Simple service for managing nonces to prevent duplicate requests."""

    def __init__(self, redis_client: RedisService):
        """Initialize the nonce service.

        Args:
            redis_client: The Redis client service used for storage.

        """
        self.redis_client = redis_client
        self.nonce_prefix = "nonce"
        self.default_ttl = 3600  # 1 hour

    def _get_nonce_key(self, nonce: str, endpoint: str) -> str:
        """Generate Redis key for nonce.

        Args:
            nonce: The unique identifier for the request.
            endpoint: The endpoint the request is directed at.

        Returns:
            str: The formatted Redis key.

        """
        return f"{self.nonce_prefix}:{endpoint}:{nonce}"

    async def is_duplicate(self, nonce: str, endpoint: str) -> bool:
        """Check if nonce was already used (returns True if duplicate).

        Args:
            nonce: The unique identifier for the request.
            endpoint: The endpoint the request is directed at.

        Returns:
            bool: True if the nonce is a duplicate, False otherwise.

        Raises:
            RuntimeError: If the Redis client is not initialized.

        """
        nonce_key = self._get_nonce_key(nonce, endpoint)

        if self.redis_client._client is None:
            raise RuntimeError("Redis client is not initialized")

        result = self.redis_client._client.set(
            nonce_key,
            "used",
            nx=True,
            ex=self.default_ttl,
        )

        return result is None
