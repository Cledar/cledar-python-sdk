"""
Simple nonce service for preventing duplicate request processing.
Uses Redis with TTL for automatic cleanup.
"""

from cledar.redis.redis import RedisService


class NonceService:
    """Simple service for managing nonces to prevent duplicate requests."""

    def __init__(self, redis_client: RedisService):
        self.redis_client = redis_client
        self.nonce_prefix = "nonce"
        self.default_ttl = 3600  # 1 hour

    def _get_nonce_key(self, nonce: str, endpoint: str) -> str:
        """Generate Redis key for nonce"""
        return f"{self.nonce_prefix}:{endpoint}:{nonce}"

    async def is_duplicate(self, nonce: str, endpoint: str) -> bool:
        """Check if nonce was already used (returns True if duplicate)"""

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
