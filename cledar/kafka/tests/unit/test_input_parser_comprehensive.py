"""
Comprehensive tests for InputParser covering JSON parsing, message validation,
error handling, and edge cases.
"""

import json
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from cledar.kafka.handlers.parser import IncorrectMessageValueError, InputParser
from cledar.kafka.models.message import KafkaMessage


class SimpleModel(BaseModel):
    """Simple test model."""

    id: str
    name: str
    value: int


class OptionalModel(BaseModel):
    """Model with optional fields."""

    id: str
    name: str | None = None
    value: int | None = None


class NestedModel(BaseModel):
    """Model with nested structure."""

    id: str
    data: dict[str, Any]
    metadata: dict[str, str] | None = None


class ComplexModel(BaseModel):
    """Complex model with various field types."""

    id: str
    count: int
    active: bool
    tags: list[str]
    config: dict[str, Any]
    nested: NestedModel | None = None


@pytest.fixture
def simple_parser() -> InputParser[SimpleModel]:
    """Create a parser for SimpleModel."""
    return InputParser(SimpleModel)


@pytest.fixture
def optional_parser() -> InputParser[OptionalModel]:
    """Create a parser for OptionalModel."""
    return InputParser(OptionalModel)


@pytest.fixture
def nested_parser() -> InputParser[NestedModel]:
    """Create a parser for NestedModel."""
    return InputParser(NestedModel)


@pytest.fixture
def complex_parser() -> InputParser[ComplexModel]:
    """Create a parser for ComplexModel."""
    return InputParser(ComplexModel)


@pytest.fixture
def valid_simple_json() -> str:
    """Valid JSON for SimpleModel."""
    return '{"id": "123", "name": "test", "value": 42}'


@pytest.fixture
def valid_nested_json() -> str:
    """Valid JSON for NestedModel."""
    return (
        '{"id": "456", "data": {"key": "value", "number": 123}, '
        '"metadata": {"type": "test"}}'
    )


@pytest.fixture
def valid_complex_json() -> str:
    """Valid JSON for ComplexModel."""
    return json.dumps(
        {
            "id": "789",
            "count": 10,
            "active": True,
            "tags": ["tag1", "tag2"],
            "config": {"setting": "value", "enabled": True},
            "nested": {
                "id": "nested-123",
                "data": {"inner": "data"},
                "metadata": {"type": "nested"},
            },
        }
    )


@pytest.fixture
def sample_message() -> KafkaMessage:
    """Create a sample KafkaMessage."""
    return KafkaMessage(
        topic="test-topic",
        value='{"id": "123", "name": "test", "value": 42}',
        key="test-key",
        offset=100,
        partition=0,
    )


def test_init() -> None:
    """Test InputParser initialization."""
    parser = InputParser(SimpleModel)
    assert parser.model == SimpleModel


def test_parse_json_valid(
    simple_parser: InputParser[SimpleModel], valid_simple_json: str
) -> None:
    """Test parsing valid JSON."""
    result = simple_parser.parse_json(valid_simple_json)

    assert isinstance(result, SimpleModel)
    assert result.id == "123"
    assert result.name == "test"
    assert result.value == 42


def test_parse_json_invalid_json(simple_parser: InputParser[SimpleModel]) -> None:
    """Test parsing invalid JSON."""
    invalid_json = '{"id": "123", "name": "test", "value": 42'  # Missing closing brace

    with pytest.raises(IncorrectMessageValueError):
        simple_parser.parse_json(invalid_json)


def test_parse_json_missing_required_field(
    simple_parser: InputParser[SimpleModel],
) -> None:
    """Test parsing JSON with missing required field."""
    incomplete_json = '{"id": "123", "name": "test"}'  # Missing 'value' field

    with pytest.raises(ValidationError):
        simple_parser.parse_json(incomplete_json)


def test_parse_json_wrong_type(simple_parser: InputParser[SimpleModel]) -> None:
    """Test parsing JSON with wrong field type."""
    wrong_type_json = '{"id": "123", "name": "test", "value": "not-a-number"}'

    with pytest.raises(ValidationError):
        simple_parser.parse_json(wrong_type_json)


def test_parse_json_extra_fields(simple_parser: InputParser[SimpleModel]) -> None:
    """Test parsing JSON with extra fields (should be ignored by default)."""
    extra_fields_json = '{"id": "123", "name": "test", "value": 42, "extra": "ignored"}'

    result = simple_parser.parse_json(extra_fields_json)

    assert isinstance(result, SimpleModel)
    assert result.id == "123"
    assert result.name == "test"
    assert result.value == 42


def test_parse_json_empty_string(simple_parser: InputParser[SimpleModel]) -> None:
    """Test parsing empty string."""
    with pytest.raises(IncorrectMessageValueError):
        simple_parser.parse_json("")


def test_parse_json_null_values(optional_parser: InputParser[OptionalModel]) -> None:
    """Test parsing JSON with null values for optional fields."""
    null_json = '{"id": "123", "name": null, "value": null}'

    result = optional_parser.parse_json(null_json)

    assert isinstance(result, OptionalModel)
    assert result.id == "123"
    assert result.name is None
    assert result.value is None


def test_parse_json_nested_structure(
    nested_parser: InputParser[NestedModel], valid_nested_json: str
) -> None:
    """Test parsing JSON with nested structure."""
    result = nested_parser.parse_json(valid_nested_json)

    assert isinstance(result, NestedModel)
    assert result.id == "456"
    assert result.data == {"key": "value", "number": 123}
    assert result.metadata == {"type": "test"}


def test_parse_json_complex_structure(
    complex_parser: InputParser[ComplexModel], valid_complex_json: str
) -> None:
    """Test parsing JSON with complex structure."""
    result = complex_parser.parse_json(valid_complex_json)

    assert isinstance(result, ComplexModel)
    assert result.id == "789"
    assert result.count == 10
    assert result.active is True
    assert result.tags == ["tag1", "tag2"]
    assert result.config == {"setting": "value", "enabled": True}
    assert result.nested is not None
    assert result.nested.id == "nested-123"


def test_parse_message_valid(
    simple_parser: InputParser[SimpleModel], sample_message: KafkaMessage
) -> None:
    """Test parsing a valid KafkaMessage."""
    result = simple_parser.parse_message(sample_message)

    assert result.key == sample_message.key
    assert result.value == sample_message.value
    assert result.topic == sample_message.topic
    assert result.offset == sample_message.offset
    assert result.partition == sample_message.partition

    assert isinstance(result.payload, SimpleModel)
    assert result.payload.id == "123"
    assert result.payload.name == "test"
    assert result.payload.value == 42


def test_parse_message_none_value(simple_parser: InputParser[SimpleModel]) -> None:
    """Test parsing a message with None value."""
    message = KafkaMessage(
        topic="test-topic",
        value=None,
        key="test-key",
        offset=100,
        partition=0,
    )

    with pytest.raises(IncorrectMessageValueError):
        simple_parser.parse_message(message)


def test_parse_message_empty_value(simple_parser: InputParser[SimpleModel]) -> None:
    """Test parsing a message with empty string value."""
    message = KafkaMessage(
        topic="test-topic",
        value="",
        key="test-key",
        offset=100,
        partition=0,
    )

    with pytest.raises(IncorrectMessageValueError):
        simple_parser.parse_message(message)


def test_parse_message_invalid_json_value(
    simple_parser: InputParser[SimpleModel],
) -> None:
    """Test parsing a message with invalid JSON value."""
    message = KafkaMessage(
        topic="test-topic",
        value='{"id": "123", "name": "test", "value": 42',  # Invalid JSON
        key="test-key",
        offset=100,
        partition=0,
    )

    with pytest.raises(IncorrectMessageValueError):
        simple_parser.parse_message(message)


def test_parse_message_missing_required_field(
    simple_parser: InputParser[SimpleModel],
) -> None:
    """Test parsing a message with missing required field."""
    message = KafkaMessage(
        topic="test-topic",
        value='{"id": "123", "name": "test"}',  # Missing 'value' field
        key="test-key",
        offset=100,
        partition=0,
    )

    with pytest.raises(ValidationError):
        simple_parser.parse_message(message)


def test_parse_message_with_special_characters(
    simple_parser: InputParser[SimpleModel],
) -> None:
    """Test parsing a message with special characters."""
    special_json = (
        '{"id": "123", "name": "test with special chars: \\n\\t\\"\'", "value": 42}'
    )
    message = KafkaMessage(
        topic="test-topic",
        value=special_json,
        key="test-key",
        offset=100,
        partition=0,
    )

    result = simple_parser.parse_message(message)

    assert result.payload.name == "test with special chars: \n\t\"'"


def test_parse_message_with_unicode(simple_parser: InputParser[SimpleModel]) -> None:
    """Test parsing a message with unicode characters."""
    unicode_json = '{"id": "123", "name": "测试名称", "value": 42}'
    message = KafkaMessage(
        topic="test-topic",
        value=unicode_json,
        key="test-key",
        offset=100,
        partition=0,
    )

    result = simple_parser.parse_message(message)

    assert result.payload.name == "测试名称"


def test_parse_message_with_large_data(nested_parser: InputParser[NestedModel]) -> None:
    """Test parsing a message with large data."""
    large_data = {"key" + str(i): "value" + str(i) for i in range(1000)}
    large_json = json.dumps(
        {"id": "123", "data": large_data, "metadata": {"type": "large"}}
    )

    message = KafkaMessage(
        topic="test-topic",
        value=large_json,
        key="test-key",
        offset=100,
        partition=0,
    )

    result = nested_parser.parse_message(message)

    assert len(result.payload.data) == 1000
    assert result.payload.data["key0"] == "value0"
    assert result.payload.data["key999"] == "value999"


def test_parse_message_with_none_key(simple_parser: InputParser[SimpleModel]) -> None:
    """Test parsing a message with None key."""
    message = KafkaMessage(
        topic="test-topic",
        value='{"id": "123", "name": "test", "value": 42}',
        key=None,
        offset=100,
        partition=0,
    )

    result = simple_parser.parse_message(message)

    assert result.key is None
    assert result.payload.id == "123"


def test_parse_message_with_none_offset_partition(
    simple_parser: InputParser[SimpleModel],
) -> None:
    """Test parsing a message with None offset and partition."""
    message = KafkaMessage(
        topic="test-topic",
        value='{"id": "123", "name": "test", "value": 42}',
        key="test-key",
        offset=None,
        partition=None,
    )

    result = simple_parser.parse_message(message)

    assert result.offset is None
    assert result.partition is None
    assert result.payload.id == "123"


def test_incorrect_message_value_error_message(
    simple_parser: InputParser[SimpleModel],
) -> None:
    """Test that IncorrectMessageValueError is raised for None value."""
    message = KafkaMessage(
        topic="test-topic",
        value=None,
        key="test-key",
        offset=100,
        partition=0,
    )

    with pytest.raises(IncorrectMessageValueError):
        simple_parser.parse_message(message)


def test_parser_with_different_models() -> None:
    """Test that parsers work with different model types."""
    simple_parser = InputParser(SimpleModel)
    optional_parser = InputParser(OptionalModel)

    simple_json = '{"id": "123", "name": "test", "value": 42}'
    optional_json = '{"id": "456"}'

    simple_result = simple_parser.parse_json(simple_json)
    optional_result = optional_parser.parse_json(optional_json)

    assert isinstance(simple_result, SimpleModel)
    assert isinstance(optional_result, OptionalModel)
    assert simple_result.id == "123"
    assert optional_result.id == "456"


def test_parse_json_with_boolean_values(
    complex_parser: InputParser[ComplexModel],
) -> None:
    """Test parsing JSON with boolean values."""
    boolean_json = (
        '{"id": "123", "count": 5, "active": true, "tags": ["tag1"], '
        '"config": {"enabled": false}}'
    )

    result = complex_parser.parse_json(boolean_json)

    assert result.active is True
    assert result.config["enabled"] is False


def test_parse_json_with_numeric_types(
    complex_parser: InputParser[ComplexModel],
) -> None:
    """Test parsing JSON with various numeric types."""
    numeric_json = (
        '{"id": "123", "count": 42, "active": true, "tags": ["tag1"], '
        '"config": {"float": 3.14, "negative": -10}}'
    )

    result = complex_parser.parse_json(numeric_json)

    assert result.count == 42
    assert result.config["float"] == 3.14
    assert result.config["negative"] == -10


def test_parse_json_with_array_types(complex_parser: InputParser[ComplexModel]) -> None:
    """Test parsing JSON with array types."""
    array_json = (
        '{"id": "123", "count": 3, "active": true, "tags": ["tag1", "tag2", "tag3"], '
        '"config": {"empty_array": []}}'
    )

    result = complex_parser.parse_json(array_json)

    assert result.tags == ["tag1", "tag2", "tag3"]
    assert result.config["empty_array"] == []


def test_parse_message_preserves_all_fields(
    simple_parser: InputParser[SimpleModel],
) -> None:
    """Test that parse_message preserves all KafkaMessage fields."""
    message = KafkaMessage(
        topic="custom-topic",
        value='{"id": "123", "name": "test", "value": 42}',
        key="custom-key",
        offset=999,
        partition=5,
    )

    result = simple_parser.parse_message(message)

    assert result.topic == "custom-topic"
    assert result.key == "custom-key"
    assert result.offset == 999
    assert result.partition == 5
    assert result.value == message.value
