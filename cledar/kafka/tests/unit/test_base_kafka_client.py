"""
Comprehensive tests for BaseKafkaClient covering edge cases, connection monitoring,
and error handling scenarios.
"""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import Consumer, KafkaException, Producer

from cledar.kafka import (
    BaseKafkaClient,
    KafkaConnectionError,
    KafkaConsumerConfig,
    KafkaConsumerNotConnectedError,
    KafkaProducerConfig,
    KafkaProducerNotConnectedError,
)


@pytest.fixture
def producer_config() -> KafkaProducerConfig:
    """Create a producer configuration for testing."""
    return KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_topic_prefix="test.",
        kafka_block_buffer_time_sec=10,
        kafka_connection_check_timeout_sec=1,
        kafka_connection_check_interval_sec=1,  # Short interval for testing
    )


@pytest.fixture
def consumer_config() -> KafkaConsumerConfig:
    """Create a consumer configuration for testing."""
    return KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_offset="latest",
        kafka_topic_prefix="test.",
        kafka_block_consumer_time_sec=1,
        kafka_connection_check_timeout_sec=1,
        kafka_auto_commit_interval_ms=1000,
        kafka_connection_check_interval_sec=1,  # Short interval for testing
    )


def test_init_with_producer_config(producer_config: KafkaProducerConfig) -> None:
    """Test BaseKafkaClient initialization with producer config."""
    client = BaseKafkaClient(config=producer_config)
    assert client.config == producer_config
    assert client.client is None
    assert client.connection_check_thread is None
    assert not client._stop_event.is_set()


def test_init_with_consumer_config(consumer_config: KafkaConsumerConfig) -> None:
    """Test BaseKafkaClient initialization with consumer config."""
    client = BaseKafkaClient(config=consumer_config)
    assert client.config == consumer_config
    assert client.client is None
    assert client.connection_check_thread is None
    assert not client._stop_event.is_set()


def test_is_alive_without_client(producer_config: KafkaProducerConfig) -> None:
    """Test is_alive returns False when client is not connected."""
    client = BaseKafkaClient(config=producer_config)
    assert not client.is_alive()


@patch("cledar.kafka.clients.base.logger")
def test_check_connection_without_client_producer(
    mock_logger: MagicMock, producer_config: KafkaProducerConfig
) -> None:
    """Test check_connection raises appropriate error for producer without client."""
    client = BaseKafkaClient(config=producer_config)

    with pytest.raises(KafkaProducerNotConnectedError):
        client.check_connection()

    mock_logger.error.assert_called_once()


@patch("cledar.kafka.clients.base.logger")
def test_check_connection_without_client_consumer(
    mock_logger: MagicMock, consumer_config: KafkaConsumerConfig
) -> None:
    """Test check_connection raises appropriate error for consumer without client."""
    client = BaseKafkaClient(config=consumer_config)

    with pytest.raises(KafkaConsumerNotConnectedError):
        client.check_connection()

    mock_logger.error.assert_called_once()


@patch("cledar.kafka.clients.base.logger")
def test_check_connection_success(
    mock_logger: MagicMock, producer_config: KafkaProducerConfig
) -> None:
    """Test successful connection check."""
    client = BaseKafkaClient(config=producer_config)
    mock_client = MagicMock(spec=Producer)
    mock_client.list_topics.return_value = None
    client.client = mock_client

    # Should not raise any exception
    client.check_connection()

    mock_client.list_topics.assert_called_once_with(
        timeout=client.config.kafka_connection_check_timeout_sec
    )


@patch("cledar.kafka.clients.base.logger")
def test_check_connection_kafka_exception(
    mock_logger: MagicMock, producer_config: KafkaProducerConfig
) -> None:
    """Test connection check with KafkaException."""
    client = BaseKafkaClient(config=producer_config)
    mock_client = MagicMock(spec=Producer)
    mock_client.list_topics.side_effect = KafkaException("Connection failed")
    client.client = mock_client

    with pytest.raises(KafkaConnectionError) as exc_info:
        client.check_connection()

    assert isinstance(exc_info.value.__cause__, KafkaException)
    mock_logger.exception.assert_called_once()


def test_is_alive_success(producer_config: KafkaProducerConfig) -> None:
    """Test is_alive returns True when connection is successful."""
    client = BaseKafkaClient(config=producer_config)
    mock_client = MagicMock(spec=Producer)
    mock_client.list_topics.return_value = None
    client.client = mock_client

    assert client.is_alive()


def test_is_alive_connection_error(producer_config: KafkaProducerConfig) -> None:
    """Test is_alive returns False when connection fails."""
    client = BaseKafkaClient(config=producer_config)
    mock_client = MagicMock(spec=Producer)
    mock_client.list_topics.side_effect = KafkaException("Connection failed")
    client.client = mock_client

    assert not client.is_alive()


def test_is_alive_not_connected_error(producer_config: KafkaProducerConfig) -> None:
    """Test is_alive returns False when client is not connected."""
    client = BaseKafkaClient(config=producer_config)
    client.client = None

    assert not client.is_alive()


@patch("threading.Thread")
@patch("cledar.kafka.clients.base.logger")
def test_start_connection_check_thread(
    mock_logger: MagicMock, mock_thread: MagicMock, producer_config: KafkaProducerConfig
) -> None:
    """Test starting connection check thread."""
    client = BaseKafkaClient(config=producer_config)
    mock_thread_instance = MagicMock()
    mock_thread.return_value = mock_thread_instance

    client.start_connection_check_thread()

    mock_thread.assert_called_once_with(target=client._monitor_connection)
    mock_thread_instance.start.assert_called_once()
    assert client.connection_check_thread == mock_thread_instance
    # Should have logged info for initialization and thread start
    assert mock_logger.info.call_count >= 1


@patch("threading.Thread")
def test_start_connection_check_thread_already_started(
    mock_thread: MagicMock, producer_config: KafkaProducerConfig
) -> None:
    """Test that starting connection check thread twice doesn't create new thread."""
    client = BaseKafkaClient(config=producer_config)
    mock_thread_instance = MagicMock()
    mock_thread.return_value = mock_thread_instance

    client.start_connection_check_thread()
    client.start_connection_check_thread()

    # Should only be called once
    mock_thread.assert_called_once()


@patch("cledar.kafka.clients.base.logger")
def test_monitor_connection_success(
    mock_logger: MagicMock, producer_config: KafkaProducerConfig
) -> None:
    """Test connection monitoring with successful checks."""
    client = BaseKafkaClient(config=producer_config)
    mock_client = MagicMock(spec=Producer)
    mock_client.list_topics.return_value = None
    client.client = mock_client

    # Set a very short interval for testing
    client.config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_connection_check_interval_sec=0,  # Use 0 for immediate execution
    )

    # Start monitoring in a separate thread
    monitor_thread = threading.Thread(target=client._monitor_connection)
    monitor_thread.start()

    # Let it run for a short time
    time.sleep(0.05)

    # Stop the monitoring
    client._stop_event.set()
    monitor_thread.join(timeout=1)

    # Should have logged successful connections
    assert any(
        "connection status: Connected" in str(call)
        for call in mock_logger.info.call_args_list
    )


@patch("cledar.kafka.clients.base.logger")
def test_monitor_connection_failure(
    mock_logger: MagicMock, producer_config: KafkaProducerConfig
) -> None:
    """Test connection monitoring with connection failures."""
    client = BaseKafkaClient(config=producer_config)
    mock_client = MagicMock(spec=Producer)
    mock_client.list_topics.side_effect = KafkaException("Connection failed")
    client.client = mock_client

    # Set a very short interval for testing
    client.config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_connection_check_interval_sec=0,  # Use 0 for immediate execution
    )

    # Start monitoring in a separate thread
    monitor_thread = threading.Thread(target=client._monitor_connection)
    monitor_thread.start()

    # Let it run for a short time
    time.sleep(0.05)

    # Stop the monitoring
    client._stop_event.set()
    monitor_thread.join(timeout=1)

    # Should have logged connection failures
    assert any(
        "connection check failed" in str(call)
        for call in mock_logger.exception.call_args_list
    )


@patch("cledar.kafka.clients.base.logger")
def test_shutdown_without_thread(
    mock_logger: MagicMock, producer_config: KafkaProducerConfig
) -> None:
    """Test shutdown when no connection check thread is running."""
    client = BaseKafkaClient(config=producer_config)
    client.shutdown()

    # Should not try to join a non-existent thread
    mock_logger.info.assert_any_call("Closing %s...", "BaseKafkaClient")


@patch("cledar.kafka.clients.base.logger")
def test_shutdown_with_producer(
    mock_logger: MagicMock, producer_config: KafkaProducerConfig
) -> None:
    """Test shutdown with producer client."""
    client = BaseKafkaClient(config=producer_config)
    mock_client = MagicMock(spec=Producer)
    mock_thread = MagicMock()

    client.client = mock_client
    client.connection_check_thread = mock_thread

    client.shutdown()

    assert client._stop_event.is_set()
    mock_thread.join.assert_called_once()
    mock_client.flush.assert_called_once_with(-1)
    mock_logger.info.assert_called()


@patch("cledar.kafka.clients.base.logger")
def test_shutdown_with_consumer(
    mock_logger: MagicMock, consumer_config: KafkaConsumerConfig
) -> None:
    """Test shutdown with consumer client."""
    client = BaseKafkaClient(config=consumer_config)
    mock_client = MagicMock(spec=Consumer)
    mock_thread = MagicMock()

    client.client = mock_client
    client.connection_check_thread = mock_thread

    client.shutdown()

    assert client._stop_event.is_set()
    mock_thread.join.assert_called_once()
    mock_client.close.assert_called_once()
    mock_logger.info.assert_called()


def test_stop_event_initialization(producer_config: KafkaProducerConfig) -> None:
    """Test that stop event is properly initialized."""
    client = BaseKafkaClient(config=producer_config)
    assert not client._stop_event.is_set()

    client._stop_event.set()
    assert client._stop_event.is_set()


def test_config_type_detection_producer(producer_config: KafkaProducerConfig) -> None:
    """Test that producer config is correctly identified."""
    client = BaseKafkaClient(config=producer_config)

    # This is tested indirectly through check_connection behavior
    with pytest.raises(KafkaProducerNotConnectedError):
        client.check_connection()


def test_config_type_detection_consumer(consumer_config: KafkaConsumerConfig) -> None:
    """Test that consumer config is correctly identified."""
    client = BaseKafkaClient(config=consumer_config)

    # This is tested indirectly through check_connection behavior
    with pytest.raises(KafkaConsumerNotConnectedError):
        client.check_connection()


@patch("cledar.kafka.clients.base.logger")
def test_post_init_logging(
    mock_logger: MagicMock, producer_config: KafkaProducerConfig
) -> None:
    """Test that __post_init__ logs initialization."""
    BaseKafkaClient(config=producer_config)

    mock_logger.info.assert_called_once()
    assert "Initializing BaseKafkaClient" in str(mock_logger.info.call_args)


def test_connection_check_timeout_configuration(
    producer_config: KafkaProducerConfig,
) -> None:
    """Test that connection check timeout is properly configured."""
    client = BaseKafkaClient(config=producer_config)
    mock_client = MagicMock(spec=Producer)
    client.client = mock_client

    client.check_connection()

    mock_client.list_topics.assert_called_once_with(
        timeout=client.config.kafka_connection_check_timeout_sec
    )


def test_connection_check_interval_configuration(
    producer_config: KafkaProducerConfig,
) -> None:
    """Test that connection check interval is properly configured."""
    client = BaseKafkaClient(config=producer_config)
    expected_interval = 5
    client.config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_connection_check_interval_sec=expected_interval,
    )

    # Test that the interval is used in _monitor_connection
    with patch.object(client._stop_event, "wait") as mock_wait:
        mock_wait.return_value = True  # Stop immediately
        client._monitor_connection()

        mock_wait.assert_called_once_with(expected_interval)
