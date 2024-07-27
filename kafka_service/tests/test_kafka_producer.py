# pylint: disable=unused-argument, protected-access
from unittest.mock import patch, MagicMock
import pytest
from confluent_kafka import KafkaException, Producer
from stream_chunker.kafka_service.kafka_producer import (
    KafkaProducer,
    KafkaProducerNotConnectedError,
)
from stream_chunker.kafka_service.base_kafka_client import KafkaConnectionError
from stream_chunker.kafka_service.utils import delivery_callback
from stream_chunker.kafka_service.schemas import KafkaProducerConfig
from stream_chunker.settings import Settings

# Constants for test
TEST_TOPIC = "test-topic"
TEST_VALUE = "test-value"
TEST_KEY = "test-key"

mock_producer_path = "stream_chunker.kafka_service.kafka_producer.Producer"


@pytest.fixture(name="config")
def fixture_config():
    settings = Settings(
        _env_file="stream_chunker/kafka_service/tests/.env.test.kafka",
        _env_file_encoding="utf-8",
    )
    return KafkaProducerConfig(
        kafka_servers=settings.kafka_servers,
        kafka_group_id=settings.kafka_group_id,
        kafka_topic_prefix=settings.kafka_topic_prefix,
        kafka_block_buffer_time_sec=settings.kafka_block_buffer_time_sec,
        kafka_connection_check_timeout_sec=settings.kafka_connection_check_timeout_sec,
        kafka_connection_check_interval_sec=(
            settings.kafka_connection_check_interval_sec
        ),
    )


@pytest.fixture(name="producer")
def fixture_producer(config):
    return KafkaProducer(config)


def test_init_producer(config, producer):
    assert isinstance(producer, KafkaProducer)
    assert producer.config == config


@patch(mock_producer_path)
@patch.object(KafkaProducer, "check_connection")
@patch.object(KafkaProducer, "start_connection_check_thread")
def test_connect(
    mock_start_connection_check_thread, mock_check_connection, mock_producer, producer
):
    producer.connect()
    mock_producer.assert_called_once_with(
        {
            "bootstrap.servers": producer.config.kafka_servers,
            "client.id": producer.config.kafka_group_id,
        }
    )
    mock_check_connection.assert_called_once()
    mock_start_connection_check_thread.assert_called_once()


@patch(mock_producer_path)
@patch.object(KafkaProducer, "check_connection")
@patch.object(KafkaProducer, "start_connection_check_thread")
def test_send(
    mock_start_connection_check_thread, mock_check_connection, mock_producer, producer
):
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
        callback=delivery_callback,
    )
    mock_producer_instance.poll.assert_called_once()


def test_send_without_connection(producer):
    with pytest.raises(KafkaProducerNotConnectedError):
        producer.send(topic=TEST_TOPIC, value=TEST_VALUE, key=TEST_KEY)


@patch(mock_producer_path)
@patch.object(KafkaProducer, "start_connection_check_thread")
def test_check_connection(mock_start_connection_check_thread, mock_producer, producer):
    mock_producer_instance = mock_producer.return_value

    producer.connect()

    mock_producer_instance.list_topics.assert_called_once_with(
        timeout=producer.config.kafka_connection_check_timeout_sec
    )


@patch(mock_producer_path)
def test_connect_failure(mock_producer, producer):
    mock_producer_instance = mock_producer.return_value
    mock_producer_instance.list_topics.side_effect = KafkaException

    with pytest.raises(KafkaConnectionError):
        producer.connect()


@patch(mock_producer_path)
@patch("threading.Thread")
@patch.object(KafkaProducer, "check_connection")
def test_start_connection_check_thread(
    mock_check_connection, mock_thread, mock_producer, producer
):
    producer.connect()
    producer.start_connection_check_thread()

    mock_thread.assert_called_once()
    assert producer.connection_check_thread is not None


@patch(mock_producer_path)
@patch("threading.Thread")
@patch.object(KafkaProducer, "check_connection")
def test_shutdown(mock_check_connection, mock_thread, mock_producer, producer):
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
