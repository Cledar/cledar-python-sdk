# Redis Service

## Purpose

The `cledar.redis` package provides a typed, high-level interface over Redis for simple key/value storage with JSON serialization, plus helpers for bulk reads and a lightweight configuration store built on Redis pub/sub and keyspace notifications.

### Key Features

- **Typed API with Pydantic**: Validate JSON payloads into Pydantic models on read
- **Async/Sync Support**: Both `AsyncRedisService` (async/await) and `RedisService` (synchronous) available
- **Safe Serialization**: Custom JSON encoder for `Enum` (to lowercase names) and `datetime` (ISO 8601)
- **Ergonomic Helpers**: `get`, `get_raw`, `set`, `list_keys`, `mget`, `delete`
- **Bulk Reads**: `mget` returns a list with typed results, `None`, or `FailedValue` for per-key errors
- **Error Mapping**: Consistent custom exceptions for connection, serialization, deserialization, and operation errors
- **Config Store**: `RedisConfigStore` with local cache, version tracking, and watchers via keyspace events
- **Well Tested**: Fast unit tests and Redis-backed integration tests

### Use Cases

- Caching results of computations as JSON documents
- Persisting lightweight application state across processes
- Reading and writing typed configuration objects
- Bulk retrieval of many keys while tolerating per-key failures
- Observing and reacting to configuration changes in near real-time
- Asynchronous I/O for high-performance applications (FastAPI, aiohttp, etc.)

## Installation

This package is part of the `cledar-python-sdk`. Install dependencies using:

```bash
# Install with uv (recommended)
uv sync --all-groups

# Or with pip (editable install from repo root)
pip install -e .
```

## Usage Examples

### Synchronous Usage

```python
from pydantic import BaseModel
from cledar.redis import RedisService, RedisServiceConfig


class UserModel(BaseModel):
    user_id: int
    name: str


# Configure and create service
config = RedisServiceConfig(
    redis_host="localhost",
    redis_port=6379,
    redis_db=0,
)
service = RedisService(config)

# Health check
assert service.is_alive() is True

# Write a typed value (automatically serialized to JSON)
user = UserModel(user_id=1, name="Alice")
service.set("user:1", user)

# Read and validate back into the model
loaded = service.get("user:1", UserModel)
print(loaded)  # UserModel(user_id=1, name='Alice')

# Raw access (no validation/decoding beyond Redis decode_responses)
service.set("greeting", "hello")
print(service.get_raw("greeting"))  # "hello"

# List keys by pattern and bulk-fetch
keys = service.list_keys("user:*")
bulk = service.mget(keys, UserModel)
# bulk is a list of UserModel | None | FailedValue

# Delete
service.delete("greeting")
```

### Asynchronous Usage

```python
import asyncio
from pydantic import BaseModel
from cledar.redis import AsyncRedisService, RedisServiceConfig


class UserModel(BaseModel):
    user_id: int
    name: str


async def main():
    # Configure and create async service
    config = RedisServiceConfig(
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
    )
    service = AsyncRedisService(config)
    await service.connect()

    try:
        # Health check
        assert await service.is_alive() is True

        # Write a typed value (automatically serialized to JSON)
        user = UserModel(user_id=1, name="Alice")
        await service.set("user:1", user)

        # Read and validate back into the model
        loaded = await service.get("user:1", UserModel)
        print(loaded)  # UserModel(user_id=1, name='Alice')

        # Raw access (no validation/decoding beyond Redis decode_responses)
        await service.set("greeting", "hello")
        print(await service.get_raw("greeting"))  # "hello"

        # List keys by pattern and bulk-fetch
        keys = await service.list_keys("user:*")
        bulk = await service.mget(keys, UserModel)
        # bulk is a list of UserModel | None | FailedValue

        # Delete
        await service.delete("greeting")

    finally:
        # Always close the connection
        await service.close()


if __name__ == "__main__":
    asyncio.run(main())
```

### FastAPI Integration Example

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from cledar.redis import AsyncRedisService, RedisServiceConfig


class UserModel(BaseModel):
    user_id: int
    name: str


# Global service instance
redis_service: AsyncRedisService | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize Redis service
    global redis_service
    config = RedisServiceConfig(
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
    )
    redis_service = AsyncRedisService(config)
    await redis_service.connect()

    yield

    # Shutdown: close Redis connection
    if redis_service:
        await redis_service.close()


app = FastAPI(lifespan=lifespan)


def get_redis() -> AsyncRedisService:
    if redis_service is None:
        raise RuntimeError("Redis service not initialized")
    return redis_service


@app.get("/users/{user_id}")
async def get_user(user_id: int, redis: AsyncRedisService = Depends(get_redis)):
    user = await redis.get(f"user:{user_id}", UserModel)
    if user is None:
        return {"error": "User not found"}
    return user


@app.post("/users")
async def create_user(user: UserModel, redis: AsyncRedisService = Depends(get_redis)):
    await redis.set(f"user:{user.user_id}", user)
    return {"status": "created", "user": user}
```

## Development

### Project Structure

```
cledar/redis/
├── __init__.py
├── exceptions.py                      # Custom exceptions
├── logger.py                          # Module logger
├── model.py                           # Base config type for RedisConfigStore
├── redis.py                           # RedisService and AsyncRedisService
├── redis_config_store.py              # Config store with caching and watchers
├── example.py                         # Small example of using RedisConfigStore
├── tests/
│   ├── test_redis_service.py          # Sync unit tests (mocked Redis)
│   ├── test_async_redis_service.py    # Async unit tests (mocked Redis)
│   ├── test_integration_redis.py      # Sync integration tests with testcontainers
│   └── test_async_integration_redis.py # Async integration tests with testcontainers
└── README.md                          # This file
```

## Running Linters

The SDK configures common linters in `pyproject.toml`.

### Installing Linters

```bash
pip install pylint mypy black

# Or with uv
uv pip install pylint mypy black
```

### Running Linters

Run these from the SDK root directory:

```bash
# From the SDK root directory
cd /path/to/cledar-python-sdk

# Pylint
pylint redis_service/

# Mypy (strict mode configured in pyproject)
mypy redis_service/

# Black (check and/or format)
black --check redis_service/
black redis_service/
```

### Run All Linters

```bash
pylint redis_service/ && \
mypy redis_service/ && \
black --check redis_service/
```

## Running Unit Tests

Unit tests use `unittest.mock` to isolate logic without a real Redis instance.

### Run All Unit Tests

```bash
# From the SDK root directory
cd /path/to/cledar-python-sdk

PYTHONPATH=$PWD uv run pytest redis_service/tests/test_redis_service.py -v
```

### Run Specific Test

```bash
PYTHONPATH=$PWD uv run pytest redis_service/tests/test_redis_service.py::test_set_with_pydantic_model_serializes_and_sets -v
```

### Unit Test Details

- **Test Framework**: pytest, pytest-asyncio
- **Mocking**: unittest.mock (sync), AsyncMock (async)
- **Test Count**: 60 unit tests (30 sync + 30 async)

## Running Integration Tests

Integration tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to run a real Redis container.

### Prerequisites

**Required**:
- Docker installed and running
- Network access to pull Docker images

### Run Integration Tests

```bash
# From the SDK root directory
cd /path/to/cledar-python-sdk

PYTHONPATH=$PWD uv run pytest redis_service/tests/test_integration_redis.py -v
```

### Integration Test Details

- **Test Framework**: pytest, pytest-asyncio + testcontainers
- **Container**: Redis
- **Image**: `redis:7.2-alpine`
- **Test Count**: 17 integration tests (8 sync + 9 async)

### Run All Tests (Unit + Integration)

```bash
PYTHONPATH=$PWD uv run pytest redis_service/tests/ -v

# With coverage
PYTHONPATH=$PWD uv run pytest redis_service/tests/ \
  --cov=redis_service \
  --cov-report=html \
  --cov-report=term \
  -v

open htmlcov/index.html
```

## CI/CD Integration

### GitLab CI Example

```yaml
test-unit:
  stage: test
  image: python:3.12
  script:
    - pip install uv
    - uv sync --all-groups
    - PYTHONPATH=$PWD uv run pytest redis_service/tests/test_redis_service.py -v

test-integration:
  stage: test
  image: python:3.12
  services:
    - docker:dind
  variables:
    DOCKER_HOST: tcp://docker:2375
    DOCKER_TLS_CERTDIR: ""
  script:
    - pip install uv
    - uv sync --all-groups
    - PYTHONPATH=$PWD uv run pytest redis_service/tests/test_integration_redis.py -v
```

### GitHub Actions Example

```yaml
name: Tests
on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.12'
      - name: Install dependencies
        run: |
          pip install uv
          uv sync --all-groups
      - name: Run unit tests
        run: PYTHONPATH=$PWD uv run pytest redis_service/tests/test_redis_service.py -v

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.12'
      - name: Install dependencies
        run: |
          pip install uv
          uv sync --all-groups
      - name: Run integration tests
        run: PYTHONPATH=$PWD uv run pytest redis_service/tests/test_integration_redis.py -v
```

## API Reference

### RedisServiceConfig

Dataclass configuring the Redis connection.

```python
from dataclasses import dataclass


@dataclass
class RedisServiceConfig:
    redis_host: str
    redis_port: int
    redis_db: int = 0
    redis_password: str | None = None
```

### RedisService (Synchronous)

High-level service over `redis.Redis` with JSON handling and typed reads.

#### Methods

- `is_alive() -> bool` — Ping Redis to check connectivity
- `set(key: str, value: Any) -> bool` — Serialize and store a value; supports dict/list, Pydantic models, primitives
- `get(key: str, model: type[T]) -> T | None` — Read and validate JSON into the given Pydantic model
- `get_raw(key: str) -> Any | None` — Read raw value (usually string) without validation
- `list_keys(pattern: str) -> list[str]` — List keys matching a glob-like pattern
- `mget(keys: list[str], model: type[T]) -> list[T | None | FailedValue]` — Bulk read with per-key error details
- `delete(key: str) -> bool` — Delete a key; returns True if a key was removed

### AsyncRedisService (Asynchronous)

High-level async service over `redis.asyncio.Redis` with JSON handling and typed reads.

#### Methods

All methods are async (use `await`):

- `connect() -> None` — Establish connection to Redis (must be called before using other methods)
- `close() -> None` — Close the Redis connection
- `is_alive() -> bool` — Ping Redis to check connectivity
- `set(key: str, value: Any) -> bool` — Serialize and store a value; supports dict/list, Pydantic models, primitives
- `get(key: str, model: type[T]) -> T | None` — Read and validate JSON into the given Pydantic model
- `get_raw(key: str) -> Any | None` — Read raw value (usually string) without validation
- `list_keys(pattern: str) -> list[str]` — List keys matching a glob-like pattern
- `mget(keys: list[str], model: type[T]) -> list[T | None | FailedValue]` — Bulk read with per-key error details
- `delete(key: str) -> bool` — Delete a key; returns True if a key was removed

#### Exceptions

- `RedisConnectionError` — Connection/transport errors
- `RedisSerializationError` — Failures before sending to Redis (e.g., unsupported object)
- `RedisDeserializationError` — Invalid JSON or model validation errors on read
- `RedisOperationError` — Other Redis command errors

### CustomEncoder

`json.JSONEncoder` subclass used internally:

- `Enum` → lowercase of member name (e.g., `Color.RED` → `"red"`)
- `datetime` → ISO 8601 string (e.g., `"2025-01-01T00:00:00"`)

### FailedValue

Dataclass used by `mget` to signal per-key errors without failing the whole call:

```python
from dataclasses import dataclass


@dataclass
class FailedValue:
    key: str
    error: Exception
```

## RedisConfigStore

`RedisConfigStore` provides a simple configuration layer on top of Redis:

- Caches last known values per key (string or list-backed history)
- Tracks a simple per-key version (`1` for string, list length for list)
- Watches keys using Redis keyspace notifications and updates local cache
- Provides `fetch`, `update`, `delete`, `versions`, `cached_version`, and `watch`

### Usage

```python
from dataclasses import dataclass
from redis import Redis
from cledar.redis.redis_config_store import RedisConfigStore
from redis_service.model import BaseConfigClass


@dataclass
class ExampleConfig(BaseConfigClass):
    name: str
    index: int
    data: dict[str, str]


r = Redis(host="localhost", port=6379, db=0, decode_responses=False)
store = RedisConfigStore(r, prefix="app:")

key = "example_config"
cfg = ExampleConfig(name="demo", index=1, data={})

# Set/update (appends new version for list-backed keys)
store[key] = cfg

# Fetch typed config and current cached version
fetched = store.fetch(ExampleConfig, key)
version = store.cached_version(key)

# Watch for updates (optional callback)
store.watch(key)
```

Note: Keyspace notifications must be enabled in Redis to receive events, for example:

```bash
# Enable keyspace and keyevent notifications (example; tailor to your needs)
redis-cli CONFIG SET notify-keyspace-events Ex
```

## Running Pre-commit Checks

```bash
uv run black redis_service/
uv run mypy redis_service/
uv run pylint redis_service/
PYTHONPATH=$PWD uv run pytest redis_service/tests/ -v
```

## License

See the main repository LICENSE file.

## Support

For issues, questions, or contributions, please refer to the main repository's contribution guidelines.



