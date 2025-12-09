"""
Comprehensive tests for error handling, retry mechanisms, and edge cases
in the Kafka service module.
"""

import time
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from cledar.kafka.clients.consumer import KafkaConsumer
from cledar.kafka.clients.producer import KafkaProducer
from cledar.kafka.config.schemas import KafkaConsumerConfig, KafkaProducerConfig
from cledar.kafka.exceptions import (
    KafkaConnectionError,
    KafkaConsumerError,
    KafkaConsumerNotConnectedError,
    KafkaProducerNotConnectedError,
)
from cledar.kafka.handlers.dead_letter import DeadLetterHandler
from cledar.kafka.handlers.parser import IncorrectMessageValueError, InputParser
from cledar.kafka.models.message import KafkaMessage


@pytest.fixture
def producer_config() -> KafkaProducerConfig:
    """Create producer configuration for error tests."""
    return KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="error-test-producer",
        kafka_topic_prefix="error-test.",
        kafka_block_buffer_time_sec=5,
        kafka_connection_check_timeout_sec=1,
        kafka_connection_check_interval_sec=30,
    )


@pytest.fixture
def consumer_config() -> KafkaConsumerConfig:
    """Create consumer configuration for error tests."""
    return KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="error-test-consumer",
        kafka_offset="earliest",
        kafka_topic_prefix="error-test.",
        kafka_block_consumer_time_sec=2,
        kafka_connection_check_timeout_sec=1,
        kafka_auto_commit_interval_ms=1000,
        kafka_connection_check_interval_sec=30,
    )


def test_producer_connection_error(producer_config: KafkaProducerConfig) -> None:
    """Test producer connection error handling."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to fail
        mock_prod_instance.list_topics.side_effect = KafkaException("Connection failed")

        producer = KafkaProducer(producer_config)

        with pytest.raises(KafkaConnectionError) as exc_info:
            producer.connect()

        assert isinstance(exc_info.value.__cause__, KafkaException)


def test_consumer_connection_error(consumer_config: KafkaConsumerConfig) -> None:
    """Test consumer connection error handling."""
    with patch("cledar.kafka.clients.consumer.Consumer") as mock_cons_class:
        mock_cons_instance = MagicMock()
        mock_cons_class.return_value = mock_cons_instance

        # Mock connection check to fail
        mock_cons_instance.list_topics.side_effect = KafkaException("Connection failed")

        consumer = KafkaConsumer(consumer_config)

        with pytest.raises(KafkaConnectionError) as exc_info:
            consumer.connect()

        assert isinstance(exc_info.value.__cause__, KafkaException)


def test_producer_send_not_connected_error(
    producer_config: KafkaProducerConfig,
) -> None:
    """Test producer send when not connected."""
    producer = KafkaProducer(producer_config)

    with pytest.raises(KafkaProducerNotConnectedError):
        producer.send(topic="test", value="test", key="key")


def test_consumer_consume_not_connected_error(
    consumer_config: KafkaConsumerConfig,
) -> None:
    """Test consumer consume when not connected."""
    consumer = KafkaConsumer(consumer_config)

    with pytest.raises(KafkaConsumerNotConnectedError):
        consumer.consume_next()


def test_consumer_subscribe_not_connected_error(
    consumer_config: KafkaConsumerConfig,
) -> None:
    """Test consumer subscribe when not connected."""
    consumer = KafkaConsumer(consumer_config)

    with pytest.raises(KafkaConsumerNotConnectedError):
        consumer.subscribe(["test-topic"])


def test_consumer_commit_not_connected_error(
    consumer_config: KafkaConsumerConfig,
) -> None:
    """Test consumer commit when not connected."""
    consumer = KafkaConsumer(consumer_config)

    message = KafkaMessage(
        topic="test",
        value="test",
        key="key",
        offset=100,
        partition=0,
    )

    with pytest.raises(KafkaConsumerNotConnectedError):
        consumer.commit(message)


def test_producer_send_kafka_exception(producer_config: KafkaProducerConfig) -> None:
    """Test producer send with KafkaException."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to succeed
        mock_prod_instance.list_topics.return_value = None

        producer = KafkaProducer(producer_config)
        producer.connect()

        # Mock produce to raise KafkaException
        mock_prod_instance.produce.side_effect = KafkaException("Produce failed")

        with pytest.raises(KafkaException):
            producer.send(topic="test", value="test", key="key")


def test_consumer_consume_kafka_exception(consumer_config: KafkaConsumerConfig) -> None:
    """Test consumer consume with KafkaException."""
    with patch("cledar.kafka.clients.consumer.Consumer") as mock_cons_class:
        mock_cons_instance = MagicMock()
        mock_cons_class.return_value = mock_cons_instance

        # Mock connection check to succeed
        mock_cons_instance.list_topics.return_value = None

        consumer = KafkaConsumer(consumer_config)
        consumer.connect()

        # Mock poll to raise KafkaException
        mock_cons_instance.poll.side_effect = KafkaException("Poll failed")

        with pytest.raises(KafkaException):
            consumer.consume_next()


def test_consumer_subscribe_kafka_exception(
    consumer_config: KafkaConsumerConfig,
) -> None:
    """Test consumer subscribe with KafkaException."""
    with patch("cledar.kafka.clients.consumer.Consumer") as mock_cons_class:
        mock_cons_instance = MagicMock()
        mock_cons_class.return_value = mock_cons_instance

        # Mock connection check to succeed
        mock_cons_instance.list_topics.return_value = None

        consumer = KafkaConsumer(consumer_config)
        consumer.connect()

        # Mock subscribe to raise KafkaException
        mock_cons_instance.subscribe.side_effect = KafkaException("Subscribe failed")

        with pytest.raises(KafkaException):
            consumer.subscribe(["test-topic"])


def test_consumer_commit_kafka_exception(consumer_config: KafkaConsumerConfig) -> None:
    """Test consumer commit with KafkaException."""
    with patch("cledar.kafka.clients.consumer.Consumer") as mock_cons_class:
        mock_cons_instance = MagicMock()
        mock_cons_class.return_value = mock_cons_instance

        # Mock connection check to succeed
        mock_cons_instance.list_topics.return_value = None

        consumer = KafkaConsumer(consumer_config)
        consumer.connect()

        # Mock commit to raise KafkaException
        mock_cons_instance.commit.side_effect = KafkaException("Commit failed")

        message = KafkaMessage(
            topic="test",
            value="test",
            key="key",
            offset=100,
            partition=0,
        )

        with pytest.raises(KafkaException):
            consumer.commit(message)


def test_producer_buffer_error_retry(producer_config: KafkaProducerConfig) -> None:
    """Test producer buffer error retry mechanism."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to succeed
        mock_prod_instance.list_topics.return_value = None

        producer = KafkaProducer(producer_config)
        producer.connect()

        # Mock produce to raise BufferError first, then succeed
        mock_prod_instance.produce.side_effect = [
            BufferError("Buffer full"),
            None,  # Success on retry
        ]

        # Should not raise exception due to retry mechanism
        producer.send(topic="test", value="test", key="key")

        # Verify produce was called twice (retry)
        assert mock_prod_instance.produce.call_count == 2
        assert (
            mock_prod_instance.poll.call_count == 1
        )  # Only called once in retry block


def test_producer_buffer_error_retry_failure(
    producer_config: KafkaProducerConfig,
) -> None:
    """Test producer buffer error retry mechanism with persistent failure."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to succeed
        mock_prod_instance.list_topics.return_value = None

        producer = KafkaProducer(producer_config)
        producer.connect()

        # Mock produce to always raise BufferError
        mock_prod_instance.produce.side_effect = BufferError("Buffer full")

        # Should raise BufferError after retry
        with pytest.raises(BufferError):
            producer.send(topic="test", value="test", key="key")

        # Verify produce was called twice (retry)
        assert mock_prod_instance.produce.call_count == 2


def test_consumer_message_error(consumer_config: KafkaConsumerConfig) -> None:
    """Test consumer message error handling."""
    with patch("cledar.kafka.clients.consumer.Consumer") as mock_cons_class:
        mock_cons_instance = MagicMock()
        mock_cons_class.return_value = mock_cons_instance

        # Mock connection check to succeed
        mock_cons_instance.list_topics.return_value = None

        consumer = KafkaConsumer(consumer_config)
        consumer.connect()

        # Mock poll to return message with error
        mock_message = MagicMock()
        mock_message.error.return_value = KafkaError(1, "Message error")
        mock_cons_instance.poll.return_value = mock_message

        with pytest.raises(KafkaConsumerError) as exc_info:
            consumer.consume_next()

        assert isinstance(exc_info.value.args[0], KafkaError)


def test_dead_letter_handler_producer_error(
    producer_config: KafkaProducerConfig,
) -> None:
    """Test DeadLetterHandler with producer error."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to succeed
        mock_prod_instance.list_topics.return_value = None

        producer = KafkaProducer(producer_config)
        producer.connect()

        # Mock send to raise KafkaException
        mock_prod_instance.produce.side_effect = KafkaException("DLQ send failed")

        dlq_handler = DeadLetterHandler(producer=producer, dlq_topic="dlq-topic")

        message = KafkaMessage(
            topic="test",
            value="test",
            key="key",
            offset=100,
            partition=0,
        )

        with pytest.raises(KafkaException):
            dlq_handler.handle(message, None)


def test_input_parser_incorrect_message_value_error() -> None:
    """Test InputParser with incorrect message value."""
    from pydantic import BaseModel

    class SimpleModel(BaseModel):
        id: str

    parser = InputParser(SimpleModel)

    message = KafkaMessage(
        topic="test",
        value=None,  # None value should cause error
        key="key",
        offset=100,
        partition=0,
    )

    with pytest.raises(IncorrectMessageValueError):
        parser.parse_message(message)


def test_input_parser_json_validation_error() -> None:
    """Test InputParser with JSON validation error."""
    from pydantic import BaseModel

    class TestModel(BaseModel):
        id: str
        value: int

    parser = InputParser(TestModel)

    message = KafkaMessage(
        topic="test",
        value='{"id": "123", "value": "not-a-number"}',  # Invalid type
        key="key",
        offset=100,
        partition=0,
    )

    from pydantic import ValidationError

    with pytest.raises(ValidationError):  # Pydantic validation error
        parser.parse_message(message)


def test_input_parser_invalid_json_error() -> None:
    """Test InputParser with invalid JSON."""
    from pydantic import BaseModel

    class TestModel(BaseModel):
        id: str
        value: int

    parser = InputParser(TestModel)

    message = KafkaMessage(
        topic="test",
        value='{"id": "123", "value": 42',  # Invalid JSON
        key="key",
        offset=100,
        partition=0,
    )

    with pytest.raises(IncorrectMessageValueError):  # JSON parsing error
        parser.parse_message(message)


def test_connection_monitoring_error_handling(
    producer_config: KafkaProducerConfig,
) -> None:
    """Test connection monitoring error handling."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to succeed initially
        mock_prod_instance.list_topics.return_value = None

        producer = KafkaProducer(producer_config)
        producer.connect()

        # Mock connection check to fail during monitoring
        mock_prod_instance.list_topics.side_effect = KafkaException("Connection lost")

        # Start monitoring thread
        producer.start_connection_check_thread()

        # Let it run for a short time
        time.sleep(0.1)

        # Stop monitoring
        producer.shutdown()

        # Should not raise exception, should handle gracefully


def test_connection_monitoring_success_recovery(
    producer_config: KafkaProducerConfig,
) -> None:
    """Test connection monitoring success recovery."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to succeed
        mock_prod_instance.list_topics.return_value = None

        producer = KafkaProducer(producer_config)
        producer.connect()

        # Start monitoring thread
        producer.start_connection_check_thread()

        # Let it run for a short time
        time.sleep(0.1)

        # Stop monitoring
        producer.shutdown()

        # Should not raise exception, should handle gracefully


def test_producer_send_with_headers_error(producer_config: KafkaProducerConfig) -> None:
    """Test producer send with headers error handling."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to succeed
        mock_prod_instance.list_topics.return_value = None

        producer = KafkaProducer(producer_config)
        producer.connect()

        # Mock produce to raise KafkaException
        mock_prod_instance.produce.side_effect = KafkaException("Headers error")

        headers = [("header1", b"value1"), ("header2", b"value2")]

        with pytest.raises(KafkaException):
            producer.send(topic="test", value="test", key="key", headers=headers)


def test_consumer_consume_with_timeout(consumer_config: KafkaConsumerConfig) -> None:
    """Test consumer consume with timeout."""
    with patch("cledar.kafka.clients.consumer.Consumer") as mock_cons_class:
        mock_cons_instance = MagicMock()
        mock_cons_class.return_value = mock_cons_instance

        # Mock connection check to succeed
        mock_cons_instance.list_topics.return_value = None

        consumer = KafkaConsumer(consumer_config)
        consumer.connect()

        # Mock poll to return None (timeout)
        mock_cons_instance.poll.return_value = None

        message = consumer.consume_next()

        assert message is None


def test_consumer_consume_with_partial_message(
    consumer_config: KafkaConsumerConfig,
) -> None:
    """Test consumer consume with partial message data."""
    with patch("cledar.kafka.clients.consumer.Consumer") as mock_cons_class:
        mock_cons_instance = MagicMock()
        mock_cons_class.return_value = mock_cons_instance

        # Mock connection check to succeed
        mock_cons_instance.list_topics.return_value = None

        consumer = KafkaConsumer(consumer_config)
        consumer.connect()

        # Mock poll to return message with partial data
        mock_message = MagicMock()
        mock_message.error.return_value = None
        mock_message.topic.return_value = "test-topic"
        mock_message.value.return_value = b"test-value"
        mock_message.key.return_value = None  # None key
        mock_message.offset.return_value = 100
        mock_message.partition.return_value = 0

        mock_cons_instance.poll.return_value = mock_message

        message = consumer.consume_next()

        assert message is not None
        assert message.value == "test-value"
        assert message.key is None


def test_producer_send_with_none_values(producer_config: KafkaProducerConfig) -> None:
    """Test producer send with None values."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to succeed
        mock_prod_instance.list_topics.return_value = None

        producer = KafkaProducer(producer_config)
        producer.connect()

        # Should not raise exception with None values
        producer.send(topic="test", value=None, key=None)

        # Verify produce was called with None values
        mock_prod_instance.produce.assert_called_once()
        call_args = mock_prod_instance.produce.call_args
        assert call_args[1]["value"] is None
        assert call_args[1]["key"] is None


def test_consumer_commit_with_none_message(
    consumer_config: KafkaConsumerConfig,
) -> None:
    """Test consumer commit with None message."""
    with patch("cledar.kafka.clients.consumer.Consumer") as mock_cons_class:
        mock_cons_instance = MagicMock()
        mock_cons_class.return_value = mock_cons_instance

        # Mock connection check to succeed
        mock_cons_instance.list_topics.return_value = None

        consumer = KafkaConsumer(consumer_config)
        consumer.connect()

        # Mock poll to return None
        mock_cons_instance.poll.return_value = None

        message = consumer.consume_next()

        assert message is None

        # Should not be able to commit None message
        # (This would be caught by type checking in real usage)


def test_producer_send_with_empty_strings(producer_config: KafkaProducerConfig) -> None:
    """Test producer send with empty strings."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to succeed
        mock_prod_instance.list_topics.return_value = None

        producer = KafkaProducer(producer_config)
        producer.connect()

        # Should not raise exception with empty strings
        producer.send(topic="", value="", key="")

        # Verify produce was called with empty strings
        mock_prod_instance.produce.assert_called_once()
        call_args = mock_prod_instance.produce.call_args
        assert call_args[1]["value"] == ""
        assert call_args[1]["key"] == ""


def test_consumer_subscribe_with_empty_topic_list(
    consumer_config: KafkaConsumerConfig,
) -> None:
    """Test consumer subscribe with empty topic list."""
    with patch("cledar.kafka.clients.consumer.Consumer") as mock_cons_class:
        mock_cons_instance = MagicMock()
        mock_cons_class.return_value = mock_cons_instance

        # Mock connection check to succeed
        mock_cons_instance.list_topics.return_value = None

        consumer = KafkaConsumer(consumer_config)
        consumer.connect()

        # Should not raise exception with empty topic list
        consumer.subscribe([])

        # Verify subscribe was called with empty list
        mock_cons_instance.subscribe.assert_called_once_with([])


def test_producer_send_with_special_characters(
    producer_config: KafkaProducerConfig,
) -> None:
    """Test producer send with special characters."""
    with patch("cledar.kafka.clients.producer.Producer") as mock_prod_class:
        mock_prod_instance = MagicMock()
        mock_prod_class.return_value = mock_prod_instance

        # Mock connection check to succeed
        mock_prod_instance.list_topics.return_value = None

        producer = KafkaProducer(producer_config)
        producer.connect()

        special_topic = "topic-with-special-chars: @#$%^&*()"
        special_value = "value-with-special-chars: @#$%^&*()"
        special_key = "key-with-special-chars: @#$%^&*()"

        # Should not raise exception with special characters
        producer.send(topic=special_topic, value=special_value, key=special_key)

        # Verify produce was called with special characters
        mock_prod_instance.produce.assert_called_once()
        call_args = mock_prod_instance.produce.call_args
        assert call_args[1]["value"] == special_value
        assert call_args[1]["key"] == special_key


def test_consumer_consume_with_unicode_characters(
    consumer_config: KafkaConsumerConfig,
) -> None:
    """Test consumer consume with unicode characters."""
    with patch("cledar.kafka.clients.consumer.Consumer") as mock_cons_class:
        mock_cons_instance = MagicMock()
        mock_cons_class.return_value = mock_cons_instance

        # Mock connection check to succeed
        mock_cons_instance.list_topics.return_value = None

        consumer = KafkaConsumer(consumer_config)
        consumer.connect()

        # Mock poll to return message with unicode
        unicode_value = "测试消息"
        unicode_key = "测试键"

        mock_message = MagicMock()
        mock_message.error.return_value = None
        mock_message.topic.return_value = "test-topic"
        mock_message.value.return_value = unicode_value.encode("utf-8")
        mock_message.key.return_value = unicode_key.encode("utf-8")
        mock_message.offset.return_value = 100
        mock_message.partition.return_value = 0

        mock_cons_instance.poll.return_value = mock_message

        message = consumer.consume_next()

        assert message is not None
        assert message.value == unicode_value
        assert message.key == unicode_key
