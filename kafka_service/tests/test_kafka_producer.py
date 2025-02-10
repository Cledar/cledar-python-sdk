# pylint: disable=unused-argument, protected-access
from unittest.mock import patch, MagicMock
import pytest
from confluent_kafka import KafkaException, Producer
from kafka_service.kafka_producer import KafkaProducer
from kafka_service.utils import delivery_callback
from kafka_service.schemas import KafkaProducerConfig
from kafka_service.exceptions import (
    KafkaProducerNotConnectedError,
    KafkaConnectionError,
)

# Constants for test
TEST_TOPIC = "test-topic"
TEST_VALUE = "test-value"
TEST_KEY = "test-key"

mock_producer_path = "kafka_service.kafka_producer.Producer"


@pytest.fixture(name="config")
def fixture_config() -> KafkaProducerConfig:
    return KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_topic_prefix="test.",
        kafka_block_buffer_time_sec=10,
        kafka_connection_check_timeout_sec=1,
        kafka_connection_check_interval_sec=60,
    )


@pytest.fixture(name="producer")
def fixture_producer(config: KafkaProducerConfig) -> KafkaProducer:
    return KafkaProducer(config)


def test_init_producer(config: KafkaProducerConfig, producer: KafkaProducer) -> None:
    assert isinstance(producer, KafkaProducer)
    assert producer.config == config


@patch(mock_producer_path)
@patch.object(KafkaProducer, "check_connection")
@patch.object(KafkaProducer, "start_connection_check_thread")
def test_connect(
    mock_start_connection_check_thread: MagicMock,
    mock_check_connection: MagicMock,
    mock_producer: MagicMock,
    producer: KafkaProducer,
) -> None:
    producer.connect()
    mock_producer.assert_called_once_with(
        {
            "bootstrap.servers": producer.config.kafka_servers,
            "client.id": producer.config.kafka_group_id,
            "compression.type": "gzip",
            "partitioner": "consistent_random",
        }
    )
    mock_check_connection.assert_called_once()
    mock_start_connection_check_thread.assert_called_once()


@patch(mock_producer_path)
@patch.object(KafkaProducer, "check_connection")
@patch.object(KafkaProducer, "start_connection_check_thread")
def test_send(
    mock_start_connection_check_thread: MagicMock,
    mock_check_connection: MagicMock,
    mock_producer: MagicMock,
    producer: KafkaProducer,
) -> None:
    mock_producer_instance = mock_producer.return_value

    producer.connect()
    producer.send(topic=TEST_TOPIC, value=TEST_VALUE, key=TEST_KEY)

    mock_producer_instance.produce.assert_called_once_with(
        topic=(
            producer.config.kafka_topic_prefix + TEST_TOPIC
            if producer.config.kafka_topic_prefix
            else TEST_TOPIC
        ),
        value=TEST_VALUE,
        key=TEST_KEY,
        headers=None,
        callback=delivery_callback,
    )
    mock_producer_instance.poll.assert_called_once()


def test_send_without_connection(producer: KafkaProducer) -> None:
    with pytest.raises(KafkaProducerNotConnectedError):
        producer.send(topic=TEST_TOPIC, value=TEST_VALUE, key=TEST_KEY)


@patch(mock_producer_path)
@patch.object(KafkaProducer, "start_connection_check_thread")
def test_check_connection(
    mock_start_connection_check_thread: MagicMock,
    mock_producer: MagicMock,
    producer: KafkaProducer,
) -> None:
    mock_producer_instance = mock_producer.return_value

    producer.connect()

    mock_producer_instance.list_topics.assert_called_once_with(
        timeout=producer.config.kafka_connection_check_timeout_sec
    )


@patch(mock_producer_path)
def test_connect_failure(mock_producer: MagicMock, producer: KafkaProducer) -> None:
    mock_producer_instance = mock_producer.return_value
    mock_producer_instance.list_topics.side_effect = KafkaException

    with pytest.raises(KafkaConnectionError):
        producer.connect()


@patch(mock_producer_path)
@patch("threading.Thread")
@patch.object(KafkaProducer, "check_connection")
def test_start_connection_check_thread(
    mock_check_connection: MagicMock,
    mock_thread: MagicMock,
    mock_producer: MagicMock,
    producer: KafkaProducer,
) -> None:
    producer.connect()
    producer.start_connection_check_thread()

    mock_thread.assert_called_once()
    assert producer.connection_check_thread is not None


@patch(mock_producer_path)
@patch("threading.Thread")
@patch.object(KafkaProducer, "check_connection")
def test_shutdown(
    mock_check_connection: MagicMock,
    mock_thread: MagicMock,
    mock_producer: MagicMock,
    producer: KafkaProducer,
) -> None:
    mock_producer_instance = MagicMock(spec=Producer)
    mock_producer.return_value = mock_producer_instance
    mock_thread_instance = MagicMock()
    mock_thread.return_value = mock_thread_instance

    producer.connect()
    producer.start_connection_check_thread()
    producer.shutdown()

    mock_producer_instance.flush.assert_called_once()
    mock_thread_instance.join.assert_called_once()
    assert producer._stop_event.is_set()
