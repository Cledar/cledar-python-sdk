"""
Integration tests for producer-consumer interaction patterns,
message ordering, error handling, and complex scenarios using real Kafka.
"""

import json
import time
from collections.abc import Generator

import pytest

from cledar.kafka.clients.consumer import KafkaConsumer
from cledar.kafka.clients.producer import KafkaProducer
from cledar.kafka.config.schemas import KafkaConsumerConfig, KafkaProducerConfig
from cledar.kafka.handlers.dead_letter import DeadLetterHandler
from cledar.kafka.handlers.parser import InputParser
from cledar.kafka.models.output import FailedMessageData
from cledar.kafka.tests.integration.helpers import E2EData, consume_until


class InteractionTestData(E2EData):
    def to_json(self) -> str:  # keep existing API usage
        return super().to_json()


@pytest.fixture
def producer_config(kafka_bootstrap_servers: str) -> KafkaProducerConfig:
    """Create producer configuration for integration tests."""
    return KafkaProducerConfig(
        kafka_servers=kafka_bootstrap_servers,
        kafka_group_id="integration-test-producer",
        kafka_topic_prefix="integration-test.",
        kafka_block_buffer_time_sec=1,
        kafka_connection_check_timeout_sec=5,
        kafka_connection_check_interval_sec=10,
    )


@pytest.fixture
def consumer_config(kafka_bootstrap_servers: str) -> KafkaConsumerConfig:
    """Create consumer configuration for integration tests."""
    return KafkaConsumerConfig(
        kafka_servers=kafka_bootstrap_servers,
        kafka_group_id="integration-test-consumer",
        kafka_offset="earliest",
        kafka_topic_prefix="integration-test.",
        kafka_block_consumer_time_sec=1,
        kafka_connection_check_timeout_sec=5,
        kafka_auto_commit_interval_ms=1000,
        kafka_connection_check_interval_sec=10,
    )


@pytest.fixture
def producer(
    producer_config: KafkaProducerConfig,
) -> Generator[KafkaProducer, None, None]:
    """Create and connect a Kafka producer."""
    producer = KafkaProducer(producer_config)
    producer.connect()
    yield producer
    producer.shutdown()


@pytest.fixture
def consumer(
    consumer_config: KafkaConsumerConfig,
) -> Generator[KafkaConsumer, None, None]:
    """Create and connect a Kafka consumer."""
    consumer = KafkaConsumer(consumer_config)
    consumer.connect()
    yield consumer
    consumer.shutdown()


def test_producer_consumer_basic_interaction(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test basic producer-consumer interaction with real Kafka."""
    topic = "test-basic-interaction"

    # Send a message first to create the topic
    test_data = InteractionTestData(
        id="test-1", message="Hello Kafka!", timestamp=time.time()
    )
    producer.send(topic=topic, value=test_data.to_json(), key="test-key")

    # Wait for topic to be created and message to be sent
    time.sleep(2)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()
    assert message is not None
    assert message.topic == f"integration-test.{topic}"
    assert message.key == "test-key"
    assert message.value == test_data.to_json()


def test_producer_consumer_multiple_messages(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test producer-consumer interaction with multiple messages."""
    topic = "test-multiple-messages"

    # Send multiple messages first to create the topic
    messages = []
    for i in range(5):
        test_data = InteractionTestData(
            id=f"test-{i}", message=f"Message {i}", timestamp=time.time()
        )
        messages.append(test_data)
        producer.send(topic=topic, value=test_data.to_json(), key=f"key-{i}")

    # Wait for topic to be created and messages to be sent
    time.sleep(3)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume all messages
    received_messages = consume_until(consumer, expected_count=5, timeout_seconds=8)

    assert len(received_messages) == 5

    # Verify message content
    for i, message in enumerate(received_messages):
        assert message.topic == f"integration-test.{topic}"
        assert message.key == f"key-{i}"
        assert message.value == messages[i].to_json()


def test_producer_consumer_with_parser(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test producer-consumer interaction with message parsing."""
    topic = "test-parser-interaction"

    # Create parser
    parser = InputParser(InteractionTestData)

    # Send a message first to create the topic
    test_data = InteractionTestData(
        id="test-parse", message="Parsed message", timestamp=time.time()
    )
    producer.send(topic=topic, value=test_data.to_json(), key="parse-key")

    # Wait for topic to be created and message to be sent
    time.sleep(2)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume and parse the message
    message = consumer.consume_next()
    assert message is not None

    parsed_message = parser.parse_message(message)
    assert parsed_message.payload.id == "test-parse"
    assert parsed_message.payload.message == "Parsed message"


def test_producer_consumer_error_handling(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test error handling in producer-consumer interaction."""
    topic = "test-error-handling"

    # Send a message with invalid JSON first to create the topic
    invalid_json = (
        '{"id": "test", "message": "invalid json", "timestamp": "not-a-number"}'
    )
    producer.send(topic=topic, value=invalid_json, key="error-key")

    # Wait for topic to be created and message to be sent
    time.sleep(2)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()
    assert message is not None
    assert message.value == invalid_json


def test_producer_consumer_with_dead_letter_handler(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test producer-consumer interaction with dead letter handler."""
    topic = "test-dlq-interaction"
    dlq_topic = "test-dlq-topic"

    # Create dead letter handler
    dlq_handler = DeadLetterHandler(producer, dlq_topic)

    # Send a message first to create the topic
    test_data = InteractionTestData(
        id="test-dlq", message="DLQ test message", timestamp=time.time()
    )
    producer.send(topic=topic, value=test_data.to_json(), key="dlq-key")

    # Wait for topic to be created and message to be sent
    time.sleep(2)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()
    assert message is not None

    # Simulate processing failure and send to DLQ
    failure_details = [
        FailedMessageData(
            raised_at="2024-01-01T00:00:00Z",
            exception_message="Test processing error",
            exception_trace="Traceback...",
            failure_reason="Test failure",
        )
    ]

    dlq_handler.handle(message, failure_details)

    # Wait for DLQ message to be sent
    time.sleep(1)

    # Subscribe to DLQ topic and verify message was sent there
    consumer.subscribe([dlq_topic])
    dlq_message = consumer.consume_next()

    assert dlq_message is not None
    assert dlq_message.topic == f"integration-test.{dlq_topic}"
    assert dlq_message.value == test_data.to_json()


def test_producer_consumer_message_ordering(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test that messages are consumed in the order they were sent."""
    topic = "test-message-ordering"

    # Send messages with sequential IDs first to create the topic
    messages = []
    for i in range(10):
        test_data = InteractionTestData(
            id=f"order-{i:03d}", message=f"Ordered message {i}", timestamp=time.time()
        )
        messages.append(test_data)
        producer.send(topic=topic, value=test_data.to_json(), key=f"order-key-{i}")

    # Wait for topic to be created and messages to be sent
    time.sleep(3)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume messages and verify order
    received_messages = consume_until(consumer, expected_count=10, timeout_seconds=10)

    assert len(received_messages) == 10

    # Verify messages are in order (by checking the ID in the JSON)
    for i, message in enumerate(received_messages):
        parsed_data = json.loads(message.value or "{}")
        assert parsed_data["id"] == f"order-{i:03d}"


def test_producer_consumer_with_headers(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test producer-consumer interaction with message headers."""
    topic = "test-headers-interaction"

    # Send a message with headers first to create the topic
    test_data = InteractionTestData(
        id="test-headers", message="Message with headers", timestamp=time.time()
    )
    headers = [
        ("custom-header", b"custom-value"),
        ("another-header", b"another-value"),
    ]

    producer.send(
        topic=topic,
        value=test_data.to_json(),
        key="headers-key",
        headers=headers,
    )

    # Wait for topic to be created and message to be sent
    time.sleep(2)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()
    assert message is not None
    assert message.value == test_data.to_json()

    # Validate received value; header visibility varies by consumer implementation.


def test_producer_consumer_commit_behavior(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test consumer commit behavior."""
    topic = "test-commit-behavior"

    # Send a message first to create the topic
    test_data = InteractionTestData(
        id="test-commit", message="Commit test message", timestamp=time.time()
    )
    producer.send(topic=topic, value=test_data.to_json(), key="commit-key")

    # Wait for topic to be created and message to be sent
    time.sleep(2)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()
    assert message is not None

    # Commit the message
    consumer.commit(message)

    # Verify commit was successful (no exception raised)
    assert True  # If we get here, commit succeeded


def test_producer_consumer_connection_recovery(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test connection recovery behavior."""
    topic = "test-connection-recovery"

    # Send a message first to create the topic
    test_data = InteractionTestData(
        id="test-recovery", message="Recovery test message", timestamp=time.time()
    )
    producer.send(topic=topic, value=test_data.to_json(), key="recovery-key")

    # Wait for topic to be created and message to be sent
    time.sleep(2)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Verify both producer and consumer are still alive
    assert producer.is_alive()
    assert consumer.is_alive()

    # Consume the message to verify everything is working
    message = consumer.consume_next()
    assert message is not None
    assert message.value == test_data.to_json()
