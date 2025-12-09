# Kafka Service

## Purpose

The `cledar.kafka` package provides typed, testable wrappers around Kafka producer and consumer clients (Confluent Kafka), together with configuration schemas, message models, parsing and dead-letter handling utilities. It is designed for clarity, reliability, and easy testing (unit and integration).

### Key Features

- **Typed Producer/Consumer**: Simple OO wrappers for Confluent Kafka
- **Pydantic Configs**: Validated, frozen dataclasses for producer/consumer configuration
- **Dead Letter Handling**: Helper to route failed messages to DLQ topics
- **Message Models**: Structured input/output models
- **Parsing Utilities**: Safe message parsing to typed payloads
- **Testability**: Comprehensive unit tests and Docker-based integration tests using testcontainers

## Installation

This package is part of the Cledar SDK. Install it using:

```bash
# Install with uv (recommended)
uv sync --all-groups

# Or with pip
pip install -e .
```

## Usage Examples

### Producer: send messages

```python
import time
from cledar.kafka.clients.producer import KafkaProducer
from cledar.kafka.config.schemas import KafkaProducerConfig

producer = KafkaProducer(
    KafkaProducerConfig(
        kafka_servers="localhost:9092",           # or ["host1:9092", "host2:9092"]
        kafka_group_id="example-producer",
        kafka_topic_prefix="my-prefix.",          # optional
        kafka_block_buffer_time_sec=1,
    )
)

producer.connect()

producer.send(
    topic="example-topic",                        # final Kafka topic will include prefix
    key="msg-1",
    value='{"id":"1","message":"hello","timestamp": %f}' % time.time(),
)

# Optionally check connection status
assert producer.is_alive()

producer.shutdown()
```

### Consumer: subscribe and consume

```python
from cledar.kafka.clients.consumer import KafkaConsumer
from cledar.kafka.config.schemas import KafkaConsumerConfig

consumer = KafkaConsumer(
    KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="example-consumer",
        kafka_offset="earliest",
        kafka_topic_prefix="my-prefix.",          # optional
        kafka_block_consumer_time_sec=1,
    )
)

consumer.connect()
consumer.subscribe(["example-topic"])            # subscribes to prefixed topic

msg = consumer.consume_next()                     # returns KafkaMessage | None
if msg is not None:
    print(msg.topic, msg.key, msg.value)

assert consumer.is_alive()
consumer.shutdown()
```

### Dead Letter Handling

```python
from cledar.kafka.handlers.dead_letter import DeadLetterHandler
from cledar.kafka.models.output import FailedMessageData

# Assume you already have a connected producer and a consumed message
handler = DeadLetterHandler(producer, dlq_topic="errors-topic")

failure_details = [
    FailedMessageData(
        raised_at="2024-01-01T00:00:00Z",
        exception_message="Processing failed",
        exception_trace="Traceback...",
        failure_reason="validation_error",
    )
]

handler.handle(message, failure_details)
```

### Parsing to Typed Payloads

```python
from pydantic import BaseModel
from cledar.kafka.handlers.parser import InputParser

class Payload(BaseModel):
    id: str
    message: str

parser = InputParser(Payload)
parsed = parser.parse_message(message)  # -> ParsedMessage[Payload]
print(parsed.payload.id, parsed.payload.message)
```

## Project Structure

```
cledar/kafka/
├── clients/
│   ├── base.py                 # BaseKafkaClient (shared logic)
│   ├── consumer.py             # KafkaConsumer wrapper
│   └── producer.py             # KafkaProducer wrapper
├── config/
│   └── schemas.py              # Pydantic frozen dataclass configs
├── handlers/
│   ├── dead_letter.py          # DeadLetterHandler
│   └── parser.py               # InputParser and related utilities
├── models/
│   ├── input.py                # Input model definitions
│   ├── message.py              # KafkaMessage, etc.
│   └── output.py               # FailedMessageData, etc.
├── utils/
│   ├── callbacks.py            # Delivery callbacks
│   ├── messages.py             # Message utilities (e.g., extract_id_from_value)
│   └── topics.py               # Topic utilities/helpers
├── logger.py                   # Module logger
└── tests/
    ├── README.md               # Tests documentation (how to run)
    ├── conftest.py             # Test-wide teardown (thread cleanup)
    ├── unit/                   # Unit tests (176)
    └── integration/            # Integration tests (41) with helpers & shared fixtures
```

## Running Linters

Common commands from repo root:

```bash
# Format (ruff)
uv run ruff format .

# Type-check (mypy)
uv run mypy kafka/

# Optional: pylint
uv run pylint kafka/
```

## Running Tests

See `kafka/tests/README.md` for full details. Quick start:

```bash
# Unit tests
PYTHONPATH=. uv run pytest kafka/tests/unit/ -v

# Integration tests (requires Docker running)
PYTHONPATH=. uv run pytest kafka/tests/integration/ -v
```

- Integration tests use `testcontainers` with Kafka image `confluentinc/cp-kafka:7.4.0`.
- Shared fixtures live in `kafka/tests/integration/conftest.py`.
- Helpers (e.g., `consume_until`) live in `kafka/tests/integration/helpers.py`.
- Test-wide teardown in `kafka/tests/conftest.py` ensures background threads do not block process exit.

## API Overview

### Configs (pydantic dataclasses)

```python
from cledar.kafka.config.schemas import KafkaProducerConfig, KafkaConsumerConfig
```

- Validated, frozen configs; construct with required `kafka_servers` and `kafka_group_id`.
- Optional fields include `kafka_topic_prefix`, timeouts, and intervals.

### Producer

```python
from cledar.kafka.clients.producer import KafkaProducer
```

- `connect()` / `shutdown()`
- `send(topic: str, value: str, key: str | None = None, headers: list[tuple[str, bytes]] | None = None)`
- `check_connection()` / `is_alive()`

### Consumer

```python
from cledar.kafka.clients.consumer import KafkaConsumer
```

- `connect()` / `shutdown()`
- `subscribe(topics: list[str])`
- `consume_next() -> KafkaMessage | None`
- `commit(message: KafkaMessage) -> None`
- `check_connection()` / `is_alive()`

### Errors

```python
from kafka.exceptions import (
    KafkaConnectionError,
    KafkaProducerNotConnectedError,
    KafkaConsumerNotConnectedError,
)
```

## Notes

- Always run tests with `PYTHONPATH=.` from the repository root to ensure imports resolve.
- Integration tests require Docker and will pull testcontainers images on first run.
- Topics are automatically prefixed with `kafka_topic_prefix` if set in configs.

## License

See the main repository LICENSE file.

## Support

For issues, questions, or contributions, please refer to the repository contribution guidelines.
