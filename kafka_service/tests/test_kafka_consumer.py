# pylint: disable=unused-argument, protected-access
from unittest.mock import patch, MagicMock
import pytest
from confluent_kafka import KafkaException, Consumer, Message
from chunk_transformer.kafka_service.kafka_consumer import (
    KafkaConsumer,
    KafkaConsumerNotConnectedError,
    KafkaConsumerError,
)
from chunk_transformer.kafka_service.base_kafka_client import KafkaConnectionError
from chunk_transformer.kafka_service.utils import build_topic
from chunk_transformer.kafka_service.schemas import KafkaConsumerConfig, KafkaMessage
from chunk_transformer.settings import Settings

# Constants for test
TEST_TOPIC = "test-topic"
TEST_VALUE = "test-value"
TEST_KEY = "test-key"
TEST_GROUP_ID = "test-group-id"

mock_consumer_path = "kafka_service.kafka_consumer.Consumer"


@pytest.fixture(name="config")
def fixture_config():
    settings = Settings(
        _env_file="kafka_service/tests/.env.test.kafka",
        _env_file_encoding="utf-8",
    )
    return KafkaConsumerConfig(
        kafka_servers=settings.kafka_servers,
        kafka_group_id=settings.kafka_group_id,
        kafka_offset=settings.kafka_reference_chunks_offset,
        kafka_topic_prefix=settings.kafka_topic_prefix,
        kafka_block_consumer_time_sec=settings.kafka_block_consumer_time_sec,
        kafka_connection_check_timeout_sec=settings.kafka_connection_check_timeout_sec,
        kafka_auto_commit_interval_ms=settings.kafka_auto_commit_interval_ms,
        kafka_connection_check_interval_sec=(
            settings.kafka_connection_check_interval_sec
        ),
    )


@pytest.fixture(name="consumer")
def fixture_consumer(config):
    return KafkaConsumer(config)


def test_init_consumer(config, consumer):
    assert isinstance(consumer, KafkaConsumer)
    assert consumer.config == config


@patch(mock_consumer_path)
@patch.object(KafkaConsumer, "check_connection")
@patch.object(KafkaConsumer, "start_connection_check_thread")
def test_connect(
    mock_start_connection_check_thread, mock_check_connection, mock_consumer, consumer
):
    consumer.connect()
    mock_consumer.assert_called_once_with(
        {
            "bootstrap.servers": consumer.config.kafka_servers,
            "enable.auto.commit": True,
            "enable.partition.eof": False,
            "auto.commit.interval.ms": consumer.config.kafka_auto_commit_interval_ms,
            "auto.offset.reset": consumer.config.kafka_offset,
            "group.id": consumer.config.kafka_group_id,
        }
    )
    mock_check_connection.assert_called_once()
    mock_start_connection_check_thread.assert_called_once()


@patch(mock_consumer_path)
def test_subscribe(mock_consumer, consumer):
    mock_consumer_instance = mock_consumer.return_value
    consumer.connect()

    consumer.subscribe([TEST_TOPIC])
    mock_consumer_instance.subscribe.assert_called_once_with(
        [build_topic(TEST_TOPIC, consumer.config.kafka_topic_prefix)]
    )


def test_subscribe_without_connection(consumer):
    with pytest.raises(KafkaConsumerNotConnectedError):
        consumer.subscribe([TEST_TOPIC])


@patch(mock_consumer_path)
def test_consume_next(mock_consumer, consumer):
    mock_consumer_instance = mock_consumer.return_value
    mock_msg = MagicMock(spec=Message)
    mock_msg.error.return_value = None
    mock_msg.topic.return_value = TEST_TOPIC
    mock_msg.value.return_value = TEST_VALUE.encode("utf-8")
    mock_msg.key.return_value = TEST_KEY.encode("utf-8")
    mock_consumer_instance.poll.return_value = mock_msg

    consumer.connect()
    consumer.subscribe([TEST_TOPIC])
    message = consumer.consume_next()

    assert message == KafkaMessage(
        topic=TEST_TOPIC,
        value=TEST_VALUE,
        key=TEST_KEY,
    )


def test_consume_next_without_connection(consumer):
    with pytest.raises(KafkaConsumerNotConnectedError):
        consumer.consume_next()


@patch(mock_consumer_path)
def test_consume_error(mock_consumer, consumer):
    mock_consumer_instance = mock_consumer.return_value
    mock_msg = MagicMock(spec=Message)
    mock_msg.error.return_value = True
    mock_consumer_instance.poll.return_value = mock_msg

    consumer.connect()
    consumer.subscribe([TEST_TOPIC])

    with pytest.raises(KafkaConsumerError):
        consumer.consume_next()


@patch(mock_consumer_path)
@patch.object(KafkaConsumer, "start_connection_check_thread")
def test_check_connection(mock_start_connection_check_thread, mock_consumer, consumer):
    mock_producer_instance = mock_consumer.return_value

    consumer.connect()

    mock_producer_instance.list_topics.assert_called_once_with(
        timeout=consumer.config.kafka_connection_check_timeout_sec
    )


@patch(mock_consumer_path)
def test_connect_failure(mock_consumer, consumer):
    mock_consumer_instance = mock_consumer.return_value
    mock_consumer_instance.list_topics.side_effect = KafkaException

    with pytest.raises(KafkaConnectionError):
        consumer.connect()


@patch(mock_consumer_path)
@patch("threading.Thread")
@patch.object(KafkaConsumer, "check_connection")
def test_start_connection_check_thread(
    mock_check_connection, mock_thread, mock_consumer, consumer
):
    consumer.connect()
    consumer.start_connection_check_thread()

    mock_thread.assert_called_once()
    assert consumer.connection_check_thread is not None


@patch(mock_consumer_path)
@patch("threading.Thread")
@patch.object(KafkaConsumer, "check_connection")
def test_shutdown(mock_check_connection, mock_thread, mock_consumer, consumer):
    mock_consumer_instance = MagicMock(spec=Consumer)
    mock_consumer.return_value = mock_consumer_instance
    mock_thread_instance = MagicMock()
    mock_thread.return_value = mock_thread_instance

    consumer.connect()
    consumer.start_connection_check_thread()
    consumer.shutdown()

    mock_consumer_instance.close.assert_called_once()
    mock_thread_instance.join.assert_called_once()
    assert consumer._stop_event.is_set()
