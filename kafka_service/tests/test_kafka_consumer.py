# pylint: disable=unused-argument, unused-variable, protected-access
from unittest.mock import patch, MagicMock
import pytest
from confluent_kafka import KafkaException, Consumer, Message
from kafka_service.kafka_consumer import KafkaConsumer
from kafka_service.exceptions import (
    KafkaConsumerNotConnectedError,
    KafkaConsumerError,
    KafkaConnectionError,
)
from kafka_service.utils import build_topic
from kafka_service.schemas import KafkaConsumerConfig, KafkaMessage

# Constants for test
TEST_TOPIC = "test-topic"
TEST_VALUE = "test-value"
TEST_KEY = "test-key"
TEST_OFFSET = 1
TEST_PARTITION = 2
TEST_GROUP_ID = "test-group-id"

mock_consumer_path = "kafka_service.kafka_consumer.Consumer"


@pytest.fixture(name="config")
def fixture_config() -> KafkaConsumerConfig:
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
def fixture_consumer(config: KafkaConsumerConfig) -> KafkaConsumer:
    return KafkaConsumer(config)


def test_init_consumer(config: KafkaConsumerConfig, consumer: KafkaConsumer) -> None:
    assert isinstance(consumer, KafkaConsumer)
    assert consumer.config == config


@patch(mock_consumer_path)
@patch.object(KafkaConsumer, "check_connection")
@patch.object(KafkaConsumer, "start_connection_check_thread")
def test_connect(
    mock_start_connection_check_thread: MagicMock,
    mock_check_connection: MagicMock,
    mock_consumer: MagicMock,
    consumer: KafkaConsumer,
) -> None:
    consumer.connect()
    mock_consumer.assert_called_once_with(
        {
            "bootstrap.servers": consumer.config.kafka_servers,
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            "auto.commit.interval.ms": consumer.config.kafka_auto_commit_interval_ms,
            "auto.offset.reset": consumer.config.kafka_offset,
            "group.id": consumer.config.kafka_group_id,
        }
    )
    mock_check_connection.assert_called_once()
    mock_start_connection_check_thread.assert_called_once()


@patch(mock_consumer_path)
def test_subscribe(mock_consumer: MagicMock, consumer: KafkaConsumer) -> None:
    mock_consumer_instance = mock_consumer.return_value
    consumer.connect()

    consumer.subscribe([TEST_TOPIC])
    mock_consumer_instance.subscribe.assert_called_once_with(
        [build_topic(TEST_TOPIC, consumer.config.kafka_topic_prefix)]
    )


def test_subscribe_without_connection(consumer: KafkaConsumer) -> None:
    with pytest.raises(KafkaConsumerNotConnectedError):
        consumer.subscribe([TEST_TOPIC])


@patch(mock_consumer_path)
def test_consume_next(mock_consumer: MagicMock, consumer: KafkaConsumer) -> None:
    mock_consumer_instance = mock_consumer.return_value
    mock_msg = MagicMock(spec=Message)
    mock_msg.error.return_value = None
    mock_msg.topic.return_value = TEST_TOPIC
    mock_msg.value.return_value = TEST_VALUE.encode("utf-8")
    mock_msg.key.return_value = TEST_KEY.encode("utf-8")
    mock_msg.offset.return_value = TEST_OFFSET
    mock_msg.partition.return_value = TEST_PARTITION
    mock_consumer_instance.poll.return_value = mock_msg

    consumer.connect()
    consumer.subscribe([TEST_TOPIC])
    message = consumer.consume_next()

    assert message == KafkaMessage(
        topic=TEST_TOPIC,
        value=TEST_VALUE,
        key=TEST_KEY,
        offset=TEST_OFFSET,
        partition=TEST_PARTITION,
    )


def test_consume_next_without_connection(consumer: KafkaConsumer) -> None:
    with pytest.raises(KafkaConsumerNotConnectedError):
        consumer.consume_next()


@patch(mock_consumer_path)
def test_consume_error(mock_consumer: MagicMock, consumer: KafkaConsumer) -> None:
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
def test_check_connection(
    mock_start_connection_check_thread: MagicMock,
    mock_consumer: MagicMock,
    consumer: KafkaConsumer,
) -> None:
    mock_producer_instance = mock_consumer.return_value

    consumer.connect()

    mock_producer_instance.list_topics.assert_called_once_with(
        timeout=consumer.config.kafka_connection_check_timeout_sec
    )


@patch(mock_consumer_path)
def test_connect_failure(mock_consumer: MagicMock, consumer: KafkaConsumer) -> None:
    mock_consumer_instance = mock_consumer.return_value
    mock_consumer_instance.list_topics.side_effect = KafkaException

    with pytest.raises(KafkaConnectionError):
        consumer.connect()


@patch(mock_consumer_path)
@patch("threading.Thread")
@patch.object(KafkaConsumer, "check_connection")
def test_start_connection_check_thread(
    mock_check_connection: MagicMock,
    mock_thread: MagicMock,
    mock_consumer: MagicMock,
    consumer: KafkaConsumer,
) -> None:
    consumer.connect()
    consumer.start_connection_check_thread()

    mock_thread.assert_called_once()
    assert consumer.connection_check_thread is not None


@patch(mock_consumer_path)
@patch("threading.Thread")
@patch.object(KafkaConsumer, "check_connection")
def test_shutdown(
    mock_check_connection: MagicMock,
    mock_thread: MagicMock,
    mock_consumer: MagicMock,
    consumer: KafkaConsumer,
) -> None:
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


@patch(mock_consumer_path)
def test_commit_success(mock_consumer: MagicMock, consumer: KafkaConsumer) -> None:
    mock_consumer_instance = mock_consumer.return_value
    mock_msg = MagicMock(spec=Message)
    mock_msg.offset.return_value = TEST_OFFSET
    mock_msg.partition.return_value = TEST_PARTITION

    consumer.connect()
    consumer.subscribe([TEST_TOPIC])

    kafka_message = KafkaMessage(
        topic=TEST_TOPIC,
        value=TEST_VALUE,
        key=TEST_KEY,
        offset=TEST_OFFSET,
        partition=TEST_PARTITION,
    )

    consumer.commit(kafka_message)

    mock_consumer_instance.commit.assert_called_once_with(asynchronous=True)


@patch(mock_consumer_path)
def test_commit_failure(mock_consumer: MagicMock, consumer: KafkaConsumer) -> None:
    mock_consumer_instance = mock_consumer.return_value
    mock_consumer_instance.commit.side_effect = KafkaException

    consumer.connect()
    consumer.subscribe([TEST_TOPIC])

    kafka_message = KafkaMessage(
        topic=TEST_TOPIC,
        value=TEST_VALUE,
        key=TEST_KEY,
        offset=TEST_OFFSET,
        partition=TEST_PARTITION,
    )

    with pytest.raises(KafkaException):
        consumer.commit(kafka_message)

    mock_consumer_instance.commit.assert_called_once_with(asynchronous=True)


def test_commit_without_connection(consumer: KafkaConsumer) -> None:
    kafka_message = KafkaMessage(
        topic=TEST_TOPIC,
        value=TEST_VALUE,
        key=TEST_KEY,
        offset=TEST_OFFSET,
        partition=TEST_PARTITION,
    )

    with pytest.raises(KafkaConsumerNotConnectedError):
        consumer.commit(kafka_message)


@patch(mock_consumer_path)
def test_consume_and_commit_after_processing(
    mock_consumer: MagicMock, consumer: KafkaConsumer
) -> None:
    mock_consumer_instance = mock_consumer.return_value
    mock_msg = MagicMock(spec=Message)
    mock_msg.error.return_value = None
    mock_msg.topic.return_value = TEST_TOPIC
    mock_msg.value.return_value = TEST_VALUE.encode("utf-8")
    mock_msg.key.return_value = TEST_KEY.encode("utf-8")
    mock_msg.offset.return_value = TEST_OFFSET
    mock_msg.partition.return_value = TEST_PARTITION
    mock_consumer_instance.poll.return_value = mock_msg
    mock_processing_function = MagicMock()

    consumer.connect()
    consumer.subscribe([TEST_TOPIC])

    message = consumer.consume_next()
    assert message is not None

    mock_processing_function(message)

    consumer.commit(message)

    mock_processing_function.assert_called_once_with(message)
    mock_consumer_instance.commit.assert_called_once_with(asynchronous=True)


def test_no_commit_on_none_message(consumer: KafkaConsumer) -> None:
    with patch.object(consumer, "consume_next", return_value=None) as mock_consume_next:
        with patch.object(consumer, "commit") as mock_commit:
            message = consumer.consume_next()
            assert message is None

            # Processing function and commit should not be called if the message is None
            mock_processing_function = MagicMock()
            mock_processing_function.assert_not_called()

            mock_commit.assert_not_called()


@patch(mock_consumer_path)
def test_commit_failure_after_processing(
    mock_consumer: MagicMock, consumer: KafkaConsumer
) -> None:
    mock_consumer_instance = mock_consumer.return_value
    mock_msg = MagicMock(spec=Message)
    mock_msg.error.return_value = None
    mock_msg.topic.return_value = TEST_TOPIC
    mock_msg.value.return_value = TEST_VALUE.encode("utf-8")
    mock_msg.key.return_value = TEST_KEY.encode("utf-8")
    mock_msg.offset.return_value = TEST_OFFSET
    mock_msg.partition.return_value = TEST_PARTITION
    mock_consumer_instance.poll.return_value = mock_msg

    mock_processing_function = MagicMock()

    mock_consumer_instance.commit.side_effect = KafkaException

    consumer.connect()
    consumer.subscribe([TEST_TOPIC])

    message = consumer.consume_next()
    assert message is not None

    mock_processing_function(message)

    with pytest.raises(KafkaException):
        consumer.commit(message)

    mock_processing_function.assert_called_once_with(message)
    mock_consumer_instance.commit.assert_called_once_with(asynchronous=True)
