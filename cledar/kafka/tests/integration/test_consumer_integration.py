"""
Integration tests for Kafka consumer using real Kafka instance.
"""

import time
from collections.abc import Generator

import pytest

from cledar.kafka.clients.consumer import KafkaConsumer
from cledar.kafka.clients.producer import KafkaProducer
from cledar.kafka.config.schemas import KafkaConsumerConfig, KafkaProducerConfig
from cledar.kafka.exceptions import KafkaConsumerNotConnectedError


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


def test_consumer_connect_and_subscribe(consumer: KafkaConsumer) -> None:
    """Test consumer connection and subscription."""
    topic = "test-consumer-basic"

    # Subscribe to topic
    consumer.subscribe([topic])

    # Verify consumer is connected
    assert consumer.is_alive()


def test_consumer_consume_messages(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test consumer message consumption."""
    topic = "test-consumer-consume"
    test_value = '{"id": "test-1", "message": "Hello Consumer!"}'
    test_key = "test-key"

    # Send message first and subscribe via helper
    producer.send(topic=topic, value=test_value, key=test_key)
    time.sleep(2)
    consumer.subscribe([topic])
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()

    assert message is not None
    assert message.topic == f"integration-test.{topic}"
    assert message.key == test_key
    assert message.value == test_value


def test_consumer_consume_multiple_messages(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test consumer consuming multiple messages."""
    topic = "test-consumer-multiple"

    # Send multiple messages first to create the topic
    messages = []
    for i in range(5):
        test_value = f'{{"id": "test-{i}", "message": "Message {i}"}}'
        test_key = f"key-{i}"
        messages.append((test_value, test_key))
        producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for topic and subscribe
    time.sleep(3)
    consumer.subscribe([topic])
    time.sleep(1)

    # Consume all messages
    received_messages = []
    for _ in range(5):
        message = consumer.consume_next()
        if message:
            received_messages.append(message)

    assert len(received_messages) == 5

    # Verify message content
    for i, message in enumerate(received_messages):
        assert message.topic == f"integration-test.{topic}"
        assert message.key == f"key-{i}"
        assert message.value == messages[i][0]


def test_consumer_commit_messages(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test consumer message committing."""
    topic = "test-consumer-commit"
    test_value = '{"id": "test-commit", "message": "Commit test"}'
    test_key = "commit-key"

    # Send message first, then subscribe
    producer.send(topic=topic, value=test_value, key=test_key)
    time.sleep(2)
    consumer.subscribe([topic])
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()
    assert message is not None

    # Commit the message
    consumer.commit(message)

    # Verify commit was successful (no exception raised)
    assert True


def test_consumer_consume_with_special_characters(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test consumer consuming messages with special characters."""
    topic = "test-consumer-special-chars"
    test_value = '{"id": "test-special", "message": "Special chars: @#$%^&*()"}'
    test_key = "special-key-with-chars: @#$%^&*()"

    # Send message first, then subscribe
    producer.send(topic=topic, value=test_value, key=test_key)
    time.sleep(2)
    consumer.subscribe([topic])
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()

    assert message is not None
    assert message.value == test_value
    assert message.key == test_key


def test_consumer_consume_with_unicode(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test consumer consuming messages with unicode characters."""
    topic = "test-consumer-unicode"
    test_value = '{"id": "test-unicode", "message": "Unicode: 测试名称"}'
    test_key = "unicode-key-测试"

    # Send message first to create the topic
    producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for topic to be created and message to be sent
    time.sleep(2)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()

    assert message is not None
    assert message.value == test_value
    assert message.key == test_key


def test_consumer_consume_large_messages(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test consumer consuming large messages."""
    topic = "test-consumer-large"

    # Create a large message
    large_data = "x" * 10000
    test_value = f'{{"id": "test-large", "data": "{large_data}"}}'
    test_key = "large-key"

    # Send message first, then subscribe
    producer.send(topic=topic, value=test_value, key=test_key)
    time.sleep(3)
    consumer.subscribe([topic])
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()

    assert message is not None
    assert message.value == test_value
    assert len(message.value) > 10000


def test_consumer_connection_check(consumer: KafkaConsumer) -> None:
    """Test consumer connection checking."""
    # Verify consumer is connected
    assert consumer.is_alive()

    # Check connection explicitly
    consumer.check_connection()

    # Should not raise any exception
    assert True


def test_consumer_not_connected_error() -> None:
    """Test consumer error when not connected."""
    config = KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
    )

    consumer = KafkaConsumer(config)

    # Should raise error when trying to subscribe without connecting
    with pytest.raises(KafkaConsumerNotConnectedError):
        consumer.subscribe(["test-topic"])


def test_consumer_shutdown(consumer_config: KafkaConsumerConfig) -> None:
    """Test consumer shutdown."""
    consumer = KafkaConsumer(consumer_config)
    consumer.connect()

    # Verify consumer is connected
    assert consumer.is_alive()

    # Shutdown consumer
    consumer.shutdown()

    # Consumer should be disconnected after shutdown
    assert not consumer.is_alive()


def test_consumer_topic_prefix(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test consumer topic prefix functionality."""
    topic = "test-prefix"
    test_value = '{"id": "test-prefix", "message": "Prefix test"}'
    test_key = "prefix-key"

    # Send message first to create the topic
    producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for topic to be created and message to be sent
    time.sleep(2)

    # Subscribe consumer to topic (should use prefix from config)
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()

    assert message is not None
    assert message.topic == f"integration-test.{topic}"
    assert message.value == test_value


def test_consumer_offset_behavior(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test consumer offset behavior."""
    topic = "test-consumer-offset"

    # Send a message first
    test_value = '{"id": "test-offset", "message": "Offset test"}'
    test_key = "offset-key"
    producer.send(topic=topic, value=test_value, key=test_key)

    # Wait and subscribe (earliest offset)
    time.sleep(2)
    consumer.subscribe([topic])
    time.sleep(1)

    # Consume the message
    message = consumer.consume_next()

    assert message is not None
    assert message.value == test_value


def test_consumer_group_behavior(
    producer: KafkaProducer, consumer_config: KafkaConsumerConfig
) -> None:
    """Test consumer group behavior."""
    topic = "test-consumer-group"
    test_value = '{"id": "test-group", "message": "Group test"}'
    test_key = "group-key"

    # Send a message first to create the topic
    producer.send(topic=topic, value=test_value, key=test_key)

    # Wait for topic to be created
    time.sleep(2)

    # Create two consumers with the same group
    consumer1 = KafkaConsumer(consumer_config)
    consumer2 = KafkaConsumer(consumer_config)

    consumer1.connect()
    consumer2.connect()

    try:
        # Subscribe both consumers to the same topic
        consumer1.subscribe([topic])
        consumer2.subscribe([topic])

        # Wait for subscription to take effect
        time.sleep(1)

        # Send another message after consumers are subscribed
        producer.send(topic=topic, value=test_value, key=test_key)

        # Wait for message to be sent
        time.sleep(1)

        # Both consumers should be alive
        assert consumer1.is_alive()
        assert consumer2.is_alive()

        # One of them should consume the message
        message1 = consumer1.consume_next()
        message2 = consumer2.consume_next()

        # At least one should have received the message
        assert message1 is not None or message2 is not None

    finally:
        consumer1.shutdown()
        consumer2.shutdown()
