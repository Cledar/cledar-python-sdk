"""
Integration tests for Kafka producer using real Kafka instance.
"""

import time
from collections.abc import Generator

import pytest

from cledar.kafka.clients.producer import KafkaProducer
from cledar.kafka.config.schemas import KafkaProducerConfig
from cledar.kafka.exceptions import KafkaProducerNotConnectedError


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
def producer(
    producer_config: KafkaProducerConfig,
) -> Generator[KafkaProducer, None, None]:
    """Create and connect a Kafka producer."""
    producer = KafkaProducer(producer_config)
    producer.connect()
    yield producer
    producer.shutdown()


def test_producer_connect_and_send(producer: KafkaProducer) -> None:
    """Test producer connection and basic message sending."""
    topic = "test-producer-basic"
    test_value = '{"id": "test-1", "message": "Hello Kafka!"}'
    test_key = "test-key"

    # Send message
    producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for message to be sent
    time.sleep(1)

    # Verify producer is still connected
    assert producer.is_alive()


def test_producer_send_multiple_messages(producer: KafkaProducer) -> None:
    """Test sending multiple messages."""
    topic = "test-producer-multiple"

    # Send multiple messages
    for i in range(5):
        test_value = f'{{"id": "test-{i}", "message": "Message {i}"}}'
        test_key = f"key-{i}"
        producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for messages to be sent
    time.sleep(2)

    # Verify producer is still connected
    assert producer.is_alive()


def test_producer_send_with_headers(producer: KafkaProducer) -> None:
    """Test sending messages with headers."""
    topic = "test-producer-headers"
    test_value = '{"id": "test-headers", "message": "Message with headers"}'
    test_key = "headers-key"
    headers = [
        ("custom-header", b"custom-value"),
        ("another-header", b"another-value"),
    ]

    # Send message with headers
    producer.send(topic=topic, value=test_value, key=test_key, headers=headers)

    # Wait for message to be sent
    time.sleep(1)

    # Verify producer is still connected
    assert producer.is_alive()


def test_producer_send_large_message(producer: KafkaProducer) -> None:
    """Test sending large messages."""
    topic = "test-producer-large"

    # Create a large message
    large_data = "x" * 10000
    test_value = f'{{"id": "test-large", "data": "{large_data}"}}'
    test_key = "large-key"

    # Send large message
    producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for message to be sent
    time.sleep(2)

    # Verify producer is still connected
    assert producer.is_alive()


def test_producer_send_with_special_characters(producer: KafkaProducer) -> None:
    """Test sending messages with special characters."""
    topic = "test-producer-special-chars"
    test_value = '{"id": "test-special", "message": "Special chars: @#$%^&*()"}'
    test_key = "special-key-with-chars: @#$%^&*()"

    # Send message with special characters
    producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for message to be sent
    time.sleep(1)

    # Verify producer is still connected
    assert producer.is_alive()


def test_producer_send_with_unicode(producer: KafkaProducer) -> None:
    """Test sending messages with unicode characters."""
    topic = "test-producer-unicode"
    test_value = '{"id": "test-unicode", "message": "Unicode: 测试名称"}'
    test_key = "unicode-key-测试"

    # Send message with unicode
    producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for message to be sent
    time.sleep(1)

    # Verify producer is still connected
    assert producer.is_alive()


def test_producer_connection_check(producer: KafkaProducer) -> None:
    """Test producer connection checking."""
    # Verify producer is connected
    assert producer.is_alive()

    # Check connection explicitly
    producer.check_connection()

    # Should not raise any exception
    assert True


def test_producer_not_connected_error() -> None:
    """Test producer error when not connected."""
    config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
    )

    producer = KafkaProducer(config)

    # Should raise error when trying to send without connecting
    with pytest.raises(KafkaProducerNotConnectedError):
        producer.send(topic="test", value="test", key="key")


def test_producer_shutdown(producer_config: KafkaProducerConfig) -> None:
    """Test producer shutdown."""
    producer = KafkaProducer(producer_config)
    producer.connect()

    # Verify producer is connected
    assert producer.is_alive()

    # Shutdown producer
    producer.shutdown()

    # Wait a moment for shutdown to complete
    time.sleep(0.5)

    # Producer should be disconnected after shutdown
    assert not producer.is_alive()


def test_producer_buffer_handling(producer: KafkaProducer) -> None:
    """Test producer buffer handling with rapid message sending."""
    topic = "test-producer-buffer"

    # Send many messages rapidly
    for i in range(100):
        test_value = f'{{"id": "buffer-test-{i}", "message": "Buffer test {i}"}}'
        test_key = f"buffer-key-{i}"
        producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for messages to be processed
    time.sleep(3)

    # Verify producer is still connected
    assert producer.is_alive()


def test_producer_topic_prefix(producer: KafkaProducer) -> None:
    """Test producer topic prefix functionality."""
    topic = "test-prefix"
    test_value = '{"id": "test-prefix", "message": "Prefix test"}'
    test_key = "prefix-key"

    # Send message (should use prefix from config)
    producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for message to be sent
    time.sleep(1)

    # Verify producer is still connected
    assert producer.is_alive()
