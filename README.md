# Cledar Python SDK

## Project Description

**Cledar Python SDK** is a shared set of production‑ready services and utilities used across Cledar projects. It can be installed from PyPI (recommended), or consumed as a Git dependency or Git submodule.

Included modules:
- kafka_service: Kafka Producer/Consumer, helpers and DLQ handler
- storage_service: Object storage abstraction (S3/ABFS/local via fsspec)
- monitoring_service: FastAPI monitoring server with Prometheus metrics and healthchecks
- redis_service: Redis‑backed typed config store
- kserve_service: KServe helpers
- common_logging: Common logging utilities
---

## Installation and Setup

1. **From PyPI (recommended)**

   Using pip:
   ```bash
   pip install cledar-sdk
   ```

   Using uv:
   ```bash
   uv add cledar-sdk
   ```

   Pin a specific version (example):
   ```bash
   pip install "cledar-sdk==1.0.1"
   ```

2. **From Git (alternative)**

   Using pip (SSH, specific tag):
   ```bash
   pip install "git+ssh://git@github.com/Cledar/cledar-python-sdk.git@v1.0.1"
   ```

   Using uv (SSH, specific tag):
   ```bash
   uv add --git ssh://git@github.com/Cledar/cledar-python-sdk.git@v1.0.1
   ```

   You can also point to a branch (e.g. `main`) instead of a tag.

3. **As a Git submodule**
   ```bash
   git submodule add git@github.com:Cledar/cledar-python-sdk.git vendor/cledar-python-sdk
   git submodule update --init --recursive
   ```
   Optionally install it in editable mode from the submodule path:
   ```bash
   uv add -e ./vendor/cledar-python-sdk
   ```

4. **Developing locally**
   ```bash
   git clone git@github.com/Cledar/cledar-python-sdk.git
   cd cledar-python-sdk
   uv sync
   ```

Python version required: 3.12.7

## Testing

Unit tests are implemented using **pytest** and **unittest**.

1. Run tests:
   ```bash
   uv run pytest
   ```

2. Adding tests:
   Place tests under each module's `tests` directory (e.g. `kafka_service/tests`, `storage_service/tests`) or create files with the `_test.py` suffix.

---

## Quick Start Examples

### Kafka
Producer:
```python
from kafka_service.clients.producer import KafkaProducer
from kafka_service.config.schemas import KafkaProducerConfig

cfg = KafkaProducerConfig(
    kafka_servers="localhost:9092",
    kafka_group_id="example",
    kafka_topic_prefix="dev",
    compression_type="snappy",
    kafka_partitioner="consistent_random",
)
producer = KafkaProducer(config=cfg)
producer.connect()
producer.send(topic="my-topic", value='{"id":"123","payload":"hello"}', key="123")
```

Consumer:
```python
from kafka_service.clients.consumer import KafkaConsumer
from kafka_service.config.schemas import KafkaConsumerConfig

cfg = KafkaConsumerConfig(
    kafka_servers="localhost:9092",
    kafka_group_id="example",
    kafka_topic_prefix="dev",
    kafka_offset="earliest",
    kafka_auto_commit_interval_ms=5000,
)
consumer = KafkaConsumer(config=cfg)
consumer.connect()
consumer.subscribe(["my-topic"])
msg = consumer.consume_next()
if msg:
    consumer.commit(msg)
```

### Object Storage (S3/ABFS/local)
```python
from storage_service.object_storage import ObjectStorageService
from storage_service.models import ObjectStorageServiceConfig

cfg = ObjectStorageServiceConfig(
    s3_access_key="minioadmin",
    s3_secret_key="minioadmin",
    s3_endpoint_url="http://localhost:9000",
)
storage = ObjectStorageService(config=cfg)
storage.upload_file(
    file_path="README.md",
    destination_path="s3://bucket/path/README.md",
)
```

### Monitoring Server
```python
from cledar.monitoring import MonitoringServer, MonitoringServerConfig

config = MonitoringServerConfig(
    readiness_checks={"s3": storage.is_alive},
    liveness_checks={"app": lambda: True},
)
server = MonitoringServer(host="0.0.0.0", port=8080, config=config)
server.start_monitoring_server()
```

### Redis Config Store
```python
from redis import Redis
from redis_service.redis_config_store import RedisConfigStore

redis = Redis(host="localhost", port=6379, db=0)
store = RedisConfigStore(redis=redis, prefix="example:")
# See redis_service/example.py for a full typed config provider example
```

## Code Quality

- **pydantic** - settings management
- **ruff**, **mypy** - Linting, formatting, and static type checking
- **pre-commit** - Pre-commit file checks

## Linting 

If you want to run linting or type checker manually, you can use the following commands. Pre-commit will run these checks automatically before each commit.
```bash
uv run ruff format .
uv run ruff check .
uv run mypy .
```

## Pre-commit setup

To get started follow these steps:

1. Install `pre-commit` by running the following command:
    ```
    pip install pre-commit
    ```

2. Once `pre-commit` is installed, set up the pre-commit hooks by running:
    ```
    pre-commit install
    ```

3. Pre-commit hooks will analyze only committed files. To analyze all files after installation run the following:
    ```
    pre-commit run --all-files
    ```


### Automatic Fixing Before Commits:
pre-commit will run Ruff (format + lint) and mypy during the commit process:

   ```bash
   git commit -m "Describe your changes"
   ```
To skip pre-commit hooks for a single commit, use the `--no-verify` flag:

    ```bash
    git commit -m "Your commit message" --no-verify
    ```

---

## Technologies and Libraries

### Main Dependencies:
 - **python** >= "3.12.7"
 - **pydantic-settings**
 - **confluent-kafka**
 - **fastapi**
 - **prometheus-client**
 - **uvicorn**
 - **redis**
 - **fsspec/s3fs/adlfs** (S3/ABFS backends)
 - **boto3** and **boto3-stubs**


### Developer Tools:
- **uv** - Dependency and environment management
- **pydantic** - settings management
- **ruff** - Linting and formatting
- **mypy** - Static type checker
- **pytest**, **unittest** - Unit tests
- **pre-commit** - Code quality hooks

---

## Commit conventions

We use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for our commit messages. This helps us to create a better, more readable changelog.

Example of a commit message:
```bash
refactor(XXX-NNN): spaghetti code is now a carbonara
```
