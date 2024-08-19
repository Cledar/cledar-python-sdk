# pylint: disable=unused-argument, protected-access
from unittest.mock import patch, MagicMock
import pytest
from confluent_kafka import KafkaException, Consumer, Message
from kafka_service.kafka_consumer import (
    KafkaConsumer,
    KafkaConsumerNotConnectedError,
    KafkaConsumerError,
)
from kafka_service.base_kafka_client import KafkaConnectionError
from kafka_service.utils import build_topic
from kafka_service.schemas import KafkaConsumerConfig, KafkaMessage

# Constants for test
TEST_TOPIC = "test-topic"
TEST_VALUE = "test-value"
TEST_KEY = "test-key"
TEST_GROUP_ID = "test-group-id"

mock_consumer_path = "kafka_service.kafka_consumer.Consumer"


@pytest.fixture(name="config")
def fixture_config():
    return KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_offset="latest",
        kafka_topic_prefix="test.",
        kafka_block_consumer_time_sec=1,
        kafka_connection_check_timeout_sec=1,
        kafka_auto_commit_interval_ms=1000,
        kafka_connection_check_interval_sec=60,
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
