"""
Comprehensive tests for utils modules covering topics, callbacks, and messages
utilities.
"""

import json
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaError

from cledar.kafka.utils.callbacks import delivery_callback
from cledar.kafka.utils.messages import (
    consumer_not_connected_msg,
    extract_id_from_value,
)
from cledar.kafka.utils.topics import build_topic


def test_build_topic_with_prefix() -> None:
    """Test building topic name with prefix."""
    result = build_topic("test-topic", "prefix.")
    assert result == "prefix.test-topic"


def test_build_topic_without_prefix() -> None:
    """Test building topic name without prefix."""
    result = build_topic("test-topic", None)
    assert result == "test-topic"


def test_build_topic_with_empty_prefix() -> None:
    """Test building topic name with empty prefix."""
    result = build_topic("test-topic", "")
    assert result == "test-topic"


def test_build_topic_with_empty_topic_name() -> None:
    """Test building topic name with empty topic name."""
    result = build_topic("", "prefix.")
    assert result == "prefix."


def test_build_topic_with_special_characters() -> None:
    """Test building topic name with special characters."""
    special_topic = "topic-with-special-chars-@#$%"
    result = build_topic(special_topic, "prefix.")
    assert result == f"prefix.{special_topic}"


def test_build_topic_with_unicode() -> None:
    """Test building topic name with unicode characters."""
    unicode_topic = "topic-with-unicode-测试"
    result = build_topic(unicode_topic, "prefix.")
    assert result == f"prefix.{unicode_topic}"


def test_build_topic_with_numbers() -> None:
    """Test building topic name with numbers."""
    numeric_topic = "topic123"
    result = build_topic(numeric_topic, "prefix.")
    assert result == f"prefix.{numeric_topic}"


def test_build_topic_with_underscores() -> None:
    """Test building topic name with underscores."""
    underscore_topic = "topic_with_underscores"
    result = build_topic(underscore_topic, "prefix.")
    assert result == f"prefix.{underscore_topic}"


def test_build_topic_with_dots_in_topic() -> None:
    """Test building topic name with dots."""
    dotted_topic = "topic.with.dots"
    result = build_topic(dotted_topic, "prefix.")
    assert result == f"prefix.{dotted_topic}"


def test_build_topic_with_dots_in_prefix() -> None:
    """Test building topic name with dots in prefix."""
    result = build_topic("test-topic", "prefix.with.dots.")
    assert result == "prefix.with.dots.test-topic"


def test_build_topic_with_spaces() -> None:
    """Test building topic name with spaces."""
    spaced_topic = "topic with spaces"
    result = build_topic(spaced_topic, "prefix.")
    assert result == f"prefix.{spaced_topic}"


def test_build_topic_with_long_names() -> None:
    """Test building topic name with long names."""
    long_topic = "a" * 1000  # Very long topic name
    long_prefix = "b" * 1000  # Very long prefix

    result = build_topic(long_topic, long_prefix)
    assert result == long_prefix + long_topic


def test_build_topic_multiple_calls() -> None:
    """Test building multiple topic names."""
    topics = ["topic1", "topic2", "topic3"]
    prefix = "prefix."

    results = [build_topic(topic, prefix) for topic in topics]

    assert results == ["prefix.topic1", "prefix.topic2", "prefix.topic3"]


@patch("cledar.kafka.utils.callbacks.logger")
def test_delivery_callback_success(mock_logger: MagicMock) -> None:
    """Test delivery callback with successful delivery."""
    mock_error = None
    mock_message = MagicMock()
    mock_message.topic.return_value = "test-topic"
    mock_message.value.return_value = b"test-value"

    delivery_callback(mock_error, mock_message)

    mock_logger.debug.assert_called_once()
    call_args = mock_logger.debug.call_args
    assert call_args[1]["extra"]["topic"] == "test-topic"


@patch("cledar.kafka.utils.callbacks.logger")
def test_delivery_callback_error(mock_logger: MagicMock) -> None:
    """Test delivery callback with error."""
    mock_error = KafkaError(1, "Test error")
    mock_message = MagicMock()
    mock_message.topic.return_value = "test-topic"

    delivery_callback(mock_error, mock_message)

    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args
    assert call_args[1]["extra"]["error"] == mock_error
    assert call_args[1]["extra"]["topic"] == "test-topic"


@patch("cledar.kafka.utils.callbacks.logger")
def test_delivery_callback_with_none_topic(mock_logger: MagicMock) -> None:
    """Test delivery callback with None topic."""
    mock_error = None
    mock_message = MagicMock()
    mock_message.topic.return_value = None

    delivery_callback(mock_error, mock_message)

    mock_logger.debug.assert_called_once()


@patch("cledar.kafka.utils.callbacks.logger")
def test_delivery_callback_with_exception_in_topic(mock_logger: MagicMock) -> None:
    """Test delivery callback with exception in topic method."""
    mock_error = None
    mock_message = MagicMock()
    mock_message.topic.side_effect = Exception("Topic error")

    delivery_callback(mock_error, mock_message)

    mock_logger.debug.assert_called_once()


@patch("cledar.kafka.utils.callbacks.logger")
def test_delivery_callback_multiple_calls(mock_logger: MagicMock) -> None:
    """Test delivery callback with multiple calls."""
    mock_error = None
    mock_message = MagicMock()
    mock_message.topic.return_value = "test-topic"

    # Call multiple times
    delivery_callback(mock_error, mock_message)
    delivery_callback(mock_error, mock_message)

    assert mock_logger.debug.call_count == 2


@patch("cledar.kafka.utils.callbacks.logger")
def test_delivery_callback_with_real_kafka_error(mock_logger: MagicMock) -> None:
    """Test delivery callback with real Kafka error."""
    mock_error = KafkaError(1, "Broker: Unknown topic or partition")
    mock_message = MagicMock()
    mock_message.topic.return_value = "test-topic"

    delivery_callback(mock_error, mock_message)

    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args
    assert call_args[1]["extra"]["error"] == mock_error


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_valid_json(mock_logger: MagicMock) -> None:
    """Test extracting ID from valid JSON."""
    valid_json = '{"id": "123", "name": "test"}'
    result = extract_id_from_value(valid_json)

    assert result == "123"
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_missing_id_field(mock_logger: MagicMock) -> None:
    """Test extracting ID from JSON without id field."""
    json_without_id = '{"name": "test", "value": 42}'
    result = extract_id_from_value(json_without_id)

    assert result == "<unknown_id>"
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_none_value(mock_logger: MagicMock) -> None:
    """Test extracting ID from None value."""
    result = extract_id_from_value(None)

    assert result == "<unknown_id>"
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_invalid_json(mock_logger: MagicMock) -> None:
    """Test extracting ID from invalid JSON."""
    invalid_json = '{"id": "123", "name": "test"'  # Missing closing brace
    result = extract_id_from_value(invalid_json)

    assert result == "<unknown_id>"
    mock_logger.error.assert_called_once()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_empty_string(mock_logger: MagicMock) -> None:
    """Test extracting ID from empty string."""
    result = extract_id_from_value("")

    assert result == "<unknown_id>"
    mock_logger.error.assert_called_once()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_non_json_string(mock_logger: MagicMock) -> None:
    """Test extracting ID from non-JSON string."""
    non_json = "This is not JSON"
    result = extract_id_from_value(non_json)

    assert result == "<unknown_id>"
    mock_logger.error.assert_called_once()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_id_is_null(mock_logger: MagicMock) -> None:
    """Test extracting ID when id field is null."""
    json_with_null_id = '{"id": null, "name": "test"}'
    result = extract_id_from_value(json_with_null_id)

    assert result == "None"  # Should convert to string
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_id_is_number(mock_logger: MagicMock) -> None:
    """Test extracting ID when id field is a number."""
    json_with_numeric_id = '{"id": 123, "name": "test"}'
    result = extract_id_from_value(json_with_numeric_id)

    assert result == "123"  # Should convert to string
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_id_is_boolean(mock_logger: MagicMock) -> None:
    """Test extracting ID when id field is a boolean."""
    json_with_boolean_id = '{"id": true, "name": "test"}'
    result = extract_id_from_value(json_with_boolean_id)

    assert result == "True"  # Should convert to string
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_id_is_array(mock_logger: MagicMock) -> None:
    """Test extracting ID when id field is an array."""
    json_with_array_id = '{"id": [1, 2, 3], "name": "test"}'
    result = extract_id_from_value(json_with_array_id)

    assert result == "[1, 2, 3]"  # Should convert to string
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_id_is_object(mock_logger: MagicMock) -> None:
    """Test extracting ID when id field is an object."""
    json_with_object_id = '{"id": {"sub": "value"}, "name": "test"}'
    result = extract_id_from_value(json_with_object_id)

    assert result == "{'sub': 'value'}"  # Should convert to string
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_empty_json(mock_logger: MagicMock) -> None:
    """Test extracting ID from empty JSON object."""
    empty_json = "{}"
    result = extract_id_from_value(empty_json)

    assert result == "<unknown_id>"
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_nested_json(mock_logger: MagicMock) -> None:
    """Test extracting ID from nested JSON."""
    nested_json = '{"data": {"id": "123", "nested": {"value": 42}}}'
    result = extract_id_from_value(nested_json)

    assert result == "<unknown_id>"  # Only looks for top-level "id"
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_with_special_characters(mock_logger: MagicMock) -> None:
    """Test extracting ID with special characters."""
    special_json = '{"id": "test@#$%^&*()", "name": "test"}'
    result = extract_id_from_value(special_json)

    assert result == "test@#$%^&*()"
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_with_unicode(mock_logger: MagicMock) -> None:
    """Test extracting ID with unicode characters."""
    unicode_json = '{"id": "测试", "name": "test"}'
    result = extract_id_from_value(unicode_json)

    assert result == "测试"
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_with_whitespace(mock_logger: MagicMock) -> None:
    """Test extracting ID with whitespace."""
    whitespace_json = '{"id": "  test  ", "name": "test"}'
    result = extract_id_from_value(whitespace_json)

    assert result == "  test  "  # Preserves whitespace
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_large_json(mock_logger: MagicMock) -> None:
    """Test extracting ID from large JSON."""
    large_data = {"key" + str(i): "value" + str(i) for i in range(1000)}
    large_json = json.dumps({"id": "123", "data": large_data})
    result = extract_id_from_value(large_json)

    assert result == "123"
    mock_logger.error.assert_not_called()


def test_consumer_not_connected_msg_constant() -> None:
    """Test that consumer_not_connected_msg is a constant."""
    assert (
        consumer_not_connected_msg
        == "KafkaConsumer is not connected. Call connect first."
    )


def test_consumer_not_connected_msg_immutable() -> None:
    """Test that consumer_not_connected_msg is a constant."""
    # The constant should always have the same value
    assert (
        consumer_not_connected_msg
        == "KafkaConsumer is not connected. Call connect first."
    )

    # Test that it's a string constant
    assert isinstance(consumer_not_connected_msg, str)
    assert len(consumer_not_connected_msg) > 0


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_multiple_calls(mock_logger: MagicMock) -> None:
    """Test extracting ID with multiple calls."""
    json1 = '{"id": "123", "name": "test1"}'
    json2 = '{"id": "456", "name": "test2"}'

    result1 = extract_id_from_value(json1)
    result2 = extract_id_from_value(json2)

    assert result1 == "123"
    assert result2 == "456"
    mock_logger.error.assert_not_called()


@patch("cledar.kafka.utils.messages.logger")
def test_extract_id_from_value_json_decode_error_logging(
    mock_logger: MagicMock,
) -> None:
    """Test that JSON decode errors are properly logged."""
    invalid_json = '{"id": "123", "name": "test"'  # Missing closing brace

    result = extract_id_from_value(invalid_json)

    assert result == "<unknown_id>"
    mock_logger.error.assert_called_once()
    error_call = mock_logger.error.call_args
    assert "Decoding for id failed" in error_call[0][0]
