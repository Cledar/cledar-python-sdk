# NonceService

Simple nonce service for preventing duplicate request processing using Redis with TTL-based automatic cleanup.

It is useful for implementing idempotency in APIs, background workers, and event processing pipelines where the same request/message may arrive more than once.

## Features
- Fast duplicate detection backed by Redis SET NX
- Per-endpoint scoping (the same nonce can be used across different endpoints)
- Automatic expiry via TTL (default 1 hour)
- Tiny API surface and easy integration

## Requirements
- Redis server accessible from your application
- Python dependencies:
  - `redis` (redis-py)
  - This repository/module (imports shown below)

## Installation
Make sure you have `redis` installed in your environment:

```bash
uv add redis
```

This module lives inside this repository. Import it directly from your code as shown in the examples below.

## Quickstart

```python
import asyncio
from cledar.redis.redis import RedisService, RedisServiceConfig
from cledar.nonce import NonceService

# 1) Create a Redis client
config = RedisServiceConfig(redis_host="localhost", redis_port=6379, redis_db=0)
redis_client = RedisService(config)

# 2) Create the NonceService
nonce_service = NonceService(redis_client)

# Optional: override default TTL (seconds)
nonce_service.default_ttl = 7200  # 2 hours

async def main():
    nonce = "request-id-123"
    endpoint = "/api/payment"  # any string identifying the endpoint or operation

    # First time: not a duplicate -> returns False
    first = await nonce_service.is_duplicate(nonce, endpoint)
    print(first)  # False

    # Second time (same nonce + same endpoint): duplicate -> returns True
    second = await nonce_service.is_duplicate(nonce, endpoint)
    print(second)  # True

    # Same nonce but a different endpoint is treated independently
    third = await nonce_service.is_duplicate(nonce, "/api/other-endpoint")
    print(third)  # False

asyncio.run(main())
```

## How it works
Under the hood, `NonceService.is_duplicate(nonce, endpoint)` performs a Redis `SET` with the flags `NX` (only set if not exists) and `EX=<TTL>`. If Redis returns that the key was set, this is the first time the nonce is seen for that endpoint (not a duplicate -> returns `False`). If the key already exists, Redis returns `None`, which the service treats as a duplicate -> returns `True`.

- Redis key format: `nonce:{endpoint}:{nonce}`
- Default TTL: `3600` seconds (1 hour); can be changed via `nonce_service.default_ttl`
- Endpoint scoping: the same nonce value can be used across different endpoints without being considered a duplicate between them

## API

```python
from cledar.redis.redis import RedisService

class NonceService:
    def __init__(self, redis_client: RedisService):
        """Simple service for managing nonces to prevent duplicate requests."""
        ...

    async def is_duplicate(self, nonce: str, endpoint: str) -> bool:
        """Return True if (nonce, endpoint) was already used within TTL."""
        ...
```

### Errors
- `RuntimeError("Redis client is not initialized")` — raised when `redis_client._client` is `None`.

## Testing
This module includes unit tests. To run them:

```bash
uv run pytest nonce_service/tests -q
```

## Best practices
- Use a stable nonce that uniquely identifies an operation (e.g., request ID, message ID)
- Choose a TTL that matches the maximum time window in which duplicates must be rejected
- Scope by a meaningful `endpoint` string to separate distinct operations
