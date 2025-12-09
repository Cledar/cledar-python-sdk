"""
End-to-end integration tests using real Kafka instance via testcontainers.
These tests require Docker to be running and may take longer to execute.
"""

import json
import time

from cledar.kafka import (
    DeadLetterHandler,
    FailedMessageData,
    InputParser,
    KafkaConsumer,
    KafkaConsumerConfig,
    KafkaProducer,
    KafkaProducerConfig,
)
from cledar.kafka.tests.integration.helpers import E2EData, consume_until


class IntegrationTestData(E2EData):
    """Alias over shared E2EData for local readability."""


"""consume_until is provided by helpers.py"""


"""Common fixtures are provided by kafka_service/tests/integration/conftest.py"""


def test_end_to_end_message_flow(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test complete end-to-end message flow."""
    topic = "test-e2e-flow"

    # Send multiple messages first to create the topic
    messages = []
    for i in range(10):
        test_data = IntegrationTestData(
            id=f"e2e-{i}", message=f"End-to-end message {i}", timestamp=time.time()
        )
        messages.append(test_data)
        producer.send(
            topic=topic, value=test_data.model_dump_json(), key=f"e2e-key-{i}"
        )

    # Wait for topic to be created and messages to be sent
    time.sleep(3)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume all messages
    received_messages = []
    for _ in range(10):
        message = consumer.consume_next()
        if message:
            received_messages.append(message)

    assert len(received_messages) == 10

    # Verify message content and order
    for i, message in enumerate(received_messages):
        assert message.topic == f"integration-test.{topic}"
        assert message.key == f"e2e-key-{i}"

        # Parse and verify message content
        parsed_data = json.loads(message.value or "{}")
        assert parsed_data["id"] == f"e2e-{i}"
        assert parsed_data["message"] == f"End-to-end message {i}"


def test_end_to_end_with_parser(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test end-to-end flow with message parsing."""
    topic = "test-e2e-parser"

    # Create parser
    parser = InputParser(IntegrationTestData)

    # Send messages first to create the topic
    test_data = IntegrationTestData(
        id="parser-test", message="Parser test message", timestamp=time.time()
    )
    producer.send(topic=topic, value=test_data.model_dump_json(), key="parser-key")

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
    assert parsed_message.payload.id == "parser-test"
    assert parsed_message.payload.message == "Parser test message"


def test_end_to_end_with_dead_letter_queue(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test end-to-end flow with dead letter queue."""
    topic = "test-e2e-dlq"
    dlq_topic = "test-e2e-dlq-topic"

    # Create dead letter handler
    dlq_handler = DeadLetterHandler(producer, dlq_topic)

    # Send a message first to create the topic
    test_data = IntegrationTestData(
        id="dlq-test", message="DLQ test message", timestamp=time.time()
    )
    producer.send(topic=topic, value=test_data.model_dump_json(), key="dlq-key")

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
    assert dlq_message.value == test_data.model_dump_json()


def test_end_to_end_error_recovery(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test end-to-end error recovery scenarios."""
    topic = "test-e2e-recovery"

    # Send messages with various error scenarios first to create the topic
    error_scenarios = [
        ('{"id": "valid", "message": "Valid message"}', "valid-key"),
        ('{"id": "invalid-json", "message": "Invalid JSON', "invalid-key"),
        ('{"id": "empty", "message": ""}', "empty-key"),
        ('{"id": "special", "message": "Special chars: @#$%^&*()"}', "special-key"),
    ]

    for value, key in error_scenarios:
        producer.send(topic=topic, value=value, key=key)

    # Wait for topic to be created and messages to be sent
    time.sleep(3)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Consume messages and verify they were all received
    received_count = 0
    for _ in range(4):
        message = consumer.consume_next()
        if message:
            received_count += 1

    assert received_count == 4


def test_end_to_end_concurrent_producers_consumers(
    producer_config: KafkaProducerConfig, consumer_config: KafkaConsumerConfig
) -> None:
    """Test end-to-end flow with multiple producers and consumers."""
    topic = "test-e2e-concurrent"

    # Create multiple producers
    producers = []
    for _i in range(3):
        producer = KafkaProducer(producer_config)
        producer.connect()
        producers.append(producer)

    # Create topic before consumers subscribe
    bootstrap_producer = producers[0]
    topic_init_data = IntegrationTestData(
        id="concurrent-init", message="init", timestamp=time.time()
    )
    bootstrap_producer.send(
        topic=topic, value=topic_init_data.model_dump_json(), key="init-key"
    )
    time.sleep(2)

    # Create multiple consumers after topic exists
    consumers = []
    for _i in range(2):
        consumer = KafkaConsumer(consumer_config)
        consumer.connect()
        consumer.subscribe([topic])
        consumers.append(consumer)

    # Wait for subscription to take effect
    time.sleep(1.5)

    # Send messages from multiple producers
    for i in range(10):
        producer_idx = i % 3
        test_data = IntegrationTestData(
            id=f"concurrent-{i}",
            message=f"Concurrent message {i}",
            timestamp=time.time(),
        )
        producers[producer_idx].send(
            topic=topic,
            value=test_data.model_dump_json(),
            key=f"concurrent-key-{i}",
        )

    # Wait for messages to be sent
    time.sleep(2)

    # Consume messages from multiple consumers with a shared deadline
    total_received = 0
    start = time.time()
    deadline = start + 10
    while total_received < 10 and time.time() < deadline:
        for c in consumers:
            msg = c.consume_next()
            if msg is not None:
                total_received += 1

    # Should have received all 10 messages across consumers
    assert total_received == 10

    # Cleanup
    for producer in producers:
        producer.shutdown()
    for consumer in consumers:
        consumer.shutdown()


def test_end_to_end_message_ordering(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test end-to-end message ordering guarantees."""
    topic = "test-e2e-ordering"

    # Send a message first to create the topic
    test_data = IntegrationTestData(
        id="order-000", message="Ordered message 0", timestamp=time.time()
    )
    producer.send(topic=topic, value=test_data.model_dump_json(), key="order-key-0")

    # Wait for topic to be created
    time.sleep(2)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Send messages with sequential IDs
    messages = []
    for i in range(20):
        test_data = IntegrationTestData(
            id=f"order-{i:03d}",
            message=f"Ordered message {i}",
            timestamp=time.time(),
        )
        messages.append(test_data)
        producer.send(
            topic=topic,
            value=test_data.model_dump_json(),
            key=f"order-key-{i}",
        )

    # Wait for messages to be sent
    time.sleep(3)

    # Consume messages and verify order
    received_messages = consume_until(consumer, expected_count=21, timeout_seconds=12)

    assert len(received_messages) == 21

    # Verify messages are in order (by checking the ID in the JSON)
    # Skip the first message (topic creation) and verify the rest
    for i, message in enumerate(received_messages[1:], 1):  # Start from index 1
        parsed_data = json.loads(message.value or "{}")
        assert parsed_data["id"] == f"order-{i - 1:03d}"  # Adjust for 0-based indexing


def test_end_to_end_connection_monitoring(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test end-to-end connection monitoring."""
    topic = "test-e2e-monitoring"

    # Send a message first to create the topic
    test_data = IntegrationTestData(
        id="monitoring-test", message="Monitoring test", timestamp=time.time()
    )
    producer.send(topic=topic, value=test_data.model_dump_json(), key="monitoring-key")

    # Wait for topic to be created
    time.sleep(2)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Send another message for the consumer to receive
    producer.send(
        topic=topic, value=test_data.model_dump_json(), key="monitoring-key-2"
    )

    # Wait for message to be sent
    time.sleep(2)

    # Verify both producer and consumer are still alive
    assert producer.is_alive()
    assert consumer.is_alive()

    # Consume the message to verify everything is working
    messages = consume_until(consumer, expected_count=1, timeout_seconds=6)
    assert len(messages) == 1
    assert messages[0].value == test_data.model_dump_json()


def test_end_to_end_large_message_handling(
    producer: KafkaProducer, consumer: KafkaConsumer
) -> None:
    """Test end-to-end handling of large messages."""
    topic = "test-e2e-large"

    # Create a large message
    large_data = "x" * 50000  # 50KB message
    test_data = IntegrationTestData(
        id="large-test", message=large_data, timestamp=time.time()
    )

    # Send large message first to create the topic
    producer.send(topic=topic, value=test_data.model_dump_json(), key="large-key")

    # Wait for topic to be created
    time.sleep(3)

    # Subscribe consumer to topic
    consumer.subscribe([topic])

    # Wait for subscription to take effect
    time.sleep(1)

    # Send another large message for the consumer to receive
    producer.send(topic=topic, value=test_data.model_dump_json(), key="large-key-2")

    # Wait for message to be sent
    time.sleep(3)

    # Consume the message
    messages = consume_until(consumer, expected_count=1, timeout_seconds=10)
    assert len(messages) == 1
    assert messages[0].value == test_data.model_dump_json()
    assert len(messages[0].value) > 50000
