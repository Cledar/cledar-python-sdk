"""
Comprehensive tests for DeadLetterHandler covering message handling,
header building, error scenarios, and edge cases.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from cledar.kafka.handlers.dead_letter import DeadLetterHandler
from cledar.kafka.models.message import KafkaMessage
from cledar.kafka.models.output import FailedMessageData


@pytest.fixture
def mock_producer() -> MagicMock:
    """Create a mock KafkaProducer for testing."""
    return MagicMock()


@pytest.fixture
def dlq_topic() -> str:
    """DLQ topic name for testing."""
    return "test-dlq-topic"


@pytest.fixture
def dead_letter_handler(mock_producer: MagicMock, dlq_topic: str) -> DeadLetterHandler:
    """Create a DeadLetterHandler instance for testing."""
    return DeadLetterHandler(producer=mock_producer, dlq_topic=dlq_topic)


@pytest.fixture
def sample_message() -> KafkaMessage:
    """Create a sample KafkaMessage for testing."""
    return KafkaMessage(
        topic="test-topic",
        value='{"id": "123", "data": "test"}',
        key="test-key",
        offset=100,
        partition=0,
    )


@pytest.fixture
def sample_failure_details() -> list[FailedMessageData]:
    """Create sample failure details for testing."""
    return [
        FailedMessageData(
            raised_at="2024-01-01T00:00:00Z",
            exception_message="Test exception",
            exception_trace="Traceback...",
            failure_reason="Processing failed",
        ),
        FailedMessageData(
            raised_at="2024-01-01T00:01:00Z",
            exception_message="Another exception",
            exception_trace="Another traceback...",
            failure_reason="Retry failed",
        ),
    ]


def test_init(mock_producer: MagicMock, dlq_topic: str) -> None:
    """Test DeadLetterHandler initialization."""
    handler = DeadLetterHandler(producer=mock_producer, dlq_topic=dlq_topic)

    assert handler.producer == mock_producer
    assert handler.dlq_topic == dlq_topic


@patch("cledar.kafka.handlers.dead_letter.logger")
def test_handle_with_failure_details(
    mock_logger: MagicMock,
    dead_letter_handler: DeadLetterHandler,
    sample_message: KafkaMessage,
    sample_failure_details: list[FailedMessageData],
) -> None:
    """Test handling a message with failure details."""
    dead_letter_handler.handle(sample_message, sample_failure_details)

    # Verify producer.send was called
    from typing import Any, cast

    producer_send = cast(Any, dead_letter_handler.producer).send
    assert producer_send.call_count == 1

    # Verify logging calls
    assert (
        mock_logger.info.call_count == 3
    )  # "Handling message", "DLQ message built", and "Message sent"

    # Verify the send call arguments
    call_args = producer_send.call_args
    assert call_args[1]["topic"] == dead_letter_handler.dlq_topic
    assert call_args[1]["value"] == sample_message.value
    assert call_args[1]["key"] == sample_message.key
    assert "headers" in call_args[1]


@patch("cledar.kafka.handlers.dead_letter.logger")
def test_handle_without_failure_details(
    mock_logger: MagicMock,
    dead_letter_handler: DeadLetterHandler,
    sample_message: KafkaMessage,
) -> None:
    """Test handling a message without failure details."""
    dead_letter_handler.handle(sample_message, None)

    # Verify producer.send was called
    from typing import Any, cast

    producer_send = cast(Any, dead_letter_handler.producer).send
    assert producer_send.call_count == 1

    # Verify logging calls
    assert mock_logger.info.call_count == 3

    # Verify the send call arguments
    call_args = producer_send.call_args
    assert call_args[1]["topic"] == dead_letter_handler.dlq_topic
    assert call_args[1]["value"] == sample_message.value
    assert call_args[1]["key"] == sample_message.key
    assert call_args[1]["headers"] == []


@patch("cledar.kafka.handlers.dead_letter.logger")
def test_handle_with_empty_failure_details(
    mock_logger: MagicMock,
    dead_letter_handler: DeadLetterHandler,
    sample_message: KafkaMessage,
) -> None:
    """Test handling a message with empty failure details list."""
    dead_letter_handler.handle(sample_message, [])

    # Verify producer.send was called
    from typing import Any, cast

    producer_send = cast(Any, dead_letter_handler.producer).send
    assert producer_send.call_count == 1

    # Verify the send call arguments
    call_args = producer_send.call_args
    assert call_args[1]["headers"] == []


def test_build_headers_with_failure_details(
    dead_letter_handler: DeadLetterHandler,
    sample_failure_details: list[FailedMessageData],
) -> None:
    """Test building headers with failure details."""
    headers = dead_letter_handler._build_headers(sample_failure_details)

    assert len(headers) == 1
    assert headers[0][0] == "failures_details"

    # Verify the JSON content
    failures_json = headers[0][1].decode("utf-8")
    failures_data = json.loads(failures_json)

    assert len(failures_data) == 2
    assert failures_data[0]["exception_message"] == "Test exception"
    assert failures_data[1]["exception_message"] == "Another exception"


def test_build_headers_without_failure_details(
    dead_letter_handler: DeadLetterHandler,
) -> None:
    """Test building headers without failure details."""
    headers = dead_letter_handler._build_headers(None)

    assert headers == []


def test_build_headers_with_empty_failure_details(
    dead_letter_handler: DeadLetterHandler,
) -> None:
    """Test building headers with empty failure details list."""
    headers = dead_letter_handler._build_headers([])

    assert headers == []


def test_build_headers_json_serialization(
    dead_letter_handler: DeadLetterHandler,
) -> None:
    """Test that failure details are properly JSON serialized."""
    failure_details = [
        FailedMessageData(
            raised_at="2024-01-01T00:00:00Z",
            exception_message="Test with special chars: \n\t\"'",
            exception_trace="Traceback with unicode: 你好",
            failure_reason="Reason with symbols: @#$%",
        )
    ]

    headers = dead_letter_handler._build_headers(failure_details)

    # Should not raise any exception during JSON serialization
    failures_json = headers[0][1].decode("utf-8")
    failures_data = json.loads(failures_json)

    assert failures_data[0]["exception_message"] == "Test with special chars: \n\t\"'"
    assert failures_data[0]["exception_trace"] == "Traceback with unicode: 你好"
    assert failures_data[0]["failure_reason"] == "Reason with symbols: @#$%"


@patch("cledar.kafka.handlers.dead_letter.logger")
def test_send_message(
    mock_logger: MagicMock,
    dead_letter_handler: DeadLetterHandler,
) -> None:
    """Test sending a DLQ message."""
    message_value = '{"test": "data"}'
    message_key = "test-key"
    headers = [("test-header", b"test-value")]

    dead_letter_handler._send_message(message_value, message_key, headers)

    # Verify producer.send was called
    # mypy: MagicMock doesn't expose precise attributes; rely on runtime assertion
    from typing import Any, cast

    producer_send = cast(Any, dead_letter_handler.producer).send
    producer_send.assert_called_once_with(
        topic=dead_letter_handler.dlq_topic,
        value=message_value,
        key=message_key,
        headers=headers,
    )

    # Verify logging
    mock_logger.info.assert_called_once()
    log_message = mock_logger.info.call_args[0][0]
    assert "Message sent to DLQ topic successfully" in log_message


@patch("cledar.kafka.handlers.dead_letter.logger")
def test_send_message_with_none_values(
    mock_logger: MagicMock,
    dead_letter_handler: DeadLetterHandler,
) -> None:
    """Test sending a DLQ message with None values."""
    dead_letter_handler._send_message(None, None, [])

    # Verify producer.send was called with None values
    # mypy: MagicMock doesn't expose precise attributes; rely on runtime assertion
    from typing import Any, cast

    producer_send = cast(Any, dead_letter_handler.producer).send
    producer_send.assert_called_once_with(
        topic=dead_letter_handler.dlq_topic,
        value=None,
        key=None,
        headers=[],
    )


def test_handle_message_with_none_value(dead_letter_handler: DeadLetterHandler) -> None:
    """Test handling a message with None value."""
    message = KafkaMessage(
        topic="test-topic",
        value=None,
        key="test-key",
        offset=100,
        partition=0,
    )

    dead_letter_handler.handle(message, None)

    # Should still send the message
    from typing import Any, cast

    producer_send = cast(Any, dead_letter_handler.producer).send
    assert producer_send.call_count == 1
    call_args = producer_send.call_args
    assert call_args[1]["value"] is None


def test_handle_message_with_empty_string_value(
    dead_letter_handler: DeadLetterHandler,
) -> None:
    """Test handling a message with empty string value."""
    message = KafkaMessage(
        topic="test-topic",
        value="",
        key="test-key",
        offset=100,
        partition=0,
    )

    dead_letter_handler.handle(message, None)

    # Should still send the message
    from typing import Any, cast

    producer_send = cast(Any, dead_letter_handler.producer).send
    assert producer_send.call_count == 1
    call_args = producer_send.call_args
    assert call_args[1]["value"] == ""


def test_handle_message_with_large_value(
    dead_letter_handler: DeadLetterHandler,
) -> None:
    """Test handling a message with large value."""
    large_value = "x" * 10000  # 10KB string
    message = KafkaMessage(
        topic="test-topic",
        value=large_value,
        key="test-key",
        offset=100,
        partition=0,
    )

    dead_letter_handler.handle(message, None)

    # Should still send the message
    from typing import Any, cast

    producer_send = cast(Any, dead_letter_handler.producer).send
    assert producer_send.call_count == 1
    call_args = producer_send.call_args
    assert call_args[1]["value"] == large_value


def test_handle_message_with_special_characters_in_key(
    dead_letter_handler: DeadLetterHandler,
) -> None:
    """Test handling a message with special characters in key."""
    special_key = "key-with-special-chars: @#$%^&*()"
    message = KafkaMessage(
        topic="test-topic",
        value='{"test": "data"}',
        key=special_key,
        offset=100,
        partition=0,
    )

    dead_letter_handler.handle(message, None)

    # Should still send the message
    from typing import Any, cast

    producer_send = cast(Any, dead_letter_handler.producer).send
    producer_send.assert_called_once()
    call_args = producer_send.call_args
    assert call_args[1]["key"] == special_key


def test_multiple_failure_details_serialization(
    dead_letter_handler: DeadLetterHandler,
) -> None:
    """Test serialization of multiple failure details."""
    failure_details = [
        FailedMessageData(
            raised_at="2024-01-01T00:00:00Z",
            exception_message="First failure",
            exception_trace="First trace",
            failure_reason="First reason",
        ),
        FailedMessageData(
            raised_at="2024-01-01T00:01:00Z",
            exception_message="Second failure",
            exception_trace="Second trace",
            failure_reason="Second reason",
        ),
        FailedMessageData(
            raised_at="2024-01-01T00:02:00Z",
            exception_message="Third failure",
            exception_trace="Third trace",
            failure_reason="Third reason",
        ),
    ]

    headers = dead_letter_handler._build_headers(failure_details)

    failures_json = headers[0][1].decode("utf-8")
    failures_data = json.loads(failures_json)

    assert len(failures_data) == 3
    assert failures_data[0]["exception_message"] == "First failure"
    assert failures_data[1]["exception_message"] == "Second failure"
    assert failures_data[2]["exception_message"] == "Third failure"


def test_failure_details_with_none_values(
    dead_letter_handler: DeadLetterHandler,
) -> None:
    """Test failure details with None values."""
    failure_details = [
        FailedMessageData(
            raised_at="2024-01-01T00:00:00Z",
            exception_message=None,
            exception_trace=None,
            failure_reason=None,
        )
    ]

    headers = dead_letter_handler._build_headers(failure_details)

    failures_json = headers[0][1].decode("utf-8")
    failures_data = json.loads(failures_json)

    assert failures_data[0]["exception_message"] is None
    assert failures_data[0]["exception_trace"] is None
    assert failures_data[0]["failure_reason"] is None


def test_dlq_topic_configuration(mock_producer: MagicMock) -> None:
    """Test that DLQ topic is properly configured."""
    custom_dlq_topic = "custom-dlq-topic"
    handler = DeadLetterHandler(producer=mock_producer, dlq_topic=custom_dlq_topic)

    message = KafkaMessage(
        topic="test-topic",
        value='{"test": "data"}',
        key="test-key",
        offset=100,
        partition=0,
    )

    handler.handle(message, None)

    from typing import Any, cast

    producer_send = cast(Any, handler.producer).send
    call_args = producer_send.call_args
    assert call_args[1]["topic"] == custom_dlq_topic


def test_producer_dependency_injection(dlq_topic: str) -> None:
    """Test that producer dependency is properly injected."""
    mock_producer1 = MagicMock()
    mock_producer2 = MagicMock()

    handler1 = DeadLetterHandler(producer=mock_producer1, dlq_topic=dlq_topic)
    handler2 = DeadLetterHandler(producer=mock_producer2, dlq_topic=dlq_topic)

    assert handler1.producer == mock_producer1
    assert handler2.producer == mock_producer2
    assert handler1.producer != handler2.producer
