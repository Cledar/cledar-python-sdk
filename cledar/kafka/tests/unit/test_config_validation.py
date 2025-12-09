"""
Comprehensive tests for configuration schemas covering validation,
edge cases, and data type handling.
"""

from dataclasses import FrozenInstanceError

import pytest
from pydantic import ValidationError

from cledar.kafka.config.schemas import (
    KafkaConsumerConfig,
    KafkaProducerConfig,
    KafkaSaslMechanism,
    KafkaSecurityProtocol,
)
from cledar.kafka.models.message import KafkaMessage


def test_kafka_message_creation() -> None:
    """Test creating a KafkaMessage with all fields."""
    message = KafkaMessage(
        topic="test-topic",
        value='{"id": "123"}',
        key="test-key",
        offset=100,
        partition=0,
    )

    assert message.topic == "test-topic"
    assert message.value == '{"id": "123"}'
    assert message.key == "test-key"
    assert message.offset == 100
    assert message.partition == 0


def test_kafka_message_with_none_values() -> None:
    """Test creating a KafkaMessage with None values."""
    message = KafkaMessage(
        topic="test-topic",
        value=None,
        key=None,
        offset=None,
        partition=None,
    )

    assert message.topic == "test-topic"
    assert message.value is None
    assert message.key is None
    assert message.offset is None
    assert message.partition is None


def test_kafka_message_with_empty_strings() -> None:
    """Test creating a KafkaMessage with empty strings."""
    message = KafkaMessage(
        topic="",
        value="",
        key="",
        offset=0,
        partition=0,
    )

    assert message.topic == ""
    assert message.value == ""
    assert message.key == ""
    assert message.offset == 0
    assert message.partition == 0


def test_kafka_message_with_special_characters() -> None:
    """Test creating a KafkaMessage with special characters."""
    special_topic = "topic-with-special-chars: @#$%^&*()"
    special_value = '{"id": "test with special chars: \n\t"\'"}'
    special_key = "key-with-special-chars: @#$%^&*()"

    message = KafkaMessage(
        topic=special_topic,
        value=special_value,
        key=special_key,
        offset=100,
        partition=0,
    )

    assert message.topic == special_topic
    assert message.value == special_value
    assert message.key == special_key


def test_kafka_message_with_unicode() -> None:
    """Test creating a KafkaMessage with unicode characters."""
    unicode_topic = "topic-with-unicode-测试"
    unicode_value = '{"id": "测试ID", "name": "测试名称"}'
    unicode_key = "key-with-unicode-测试"

    message = KafkaMessage(
        topic=unicode_topic,
        value=unicode_value,
        key=unicode_key,
        offset=100,
        partition=0,
    )

    assert message.topic == unicode_topic
    assert message.value == unicode_value
    assert message.key == unicode_key


def test_kafka_message_with_large_values() -> None:
    """Test creating a KafkaMessage with large values."""
    large_value = '{"data": "' + "x" * 10000 + '"}'

    message = KafkaMessage(
        topic="test-topic",
        value=large_value,
        key="test-key",
        offset=100,
        partition=0,
    )

    assert message.value == large_value
    assert len(message.value) > 10000


def test_kafka_message_equality() -> None:
    """Test KafkaMessage equality comparison."""
    message1 = KafkaMessage(
        topic="test-topic",
        value='{"id": "123"}',
        key="test-key",
        offset=100,
        partition=0,
    )

    message2 = KafkaMessage(
        topic="test-topic",
        value='{"id": "123"}',
        key="test-key",
        offset=100,
        partition=0,
    )

    assert message1 == message2


def test_kafka_message_inequality() -> None:
    """Test KafkaMessage inequality comparison."""
    message1 = KafkaMessage(
        topic="test-topic",
        value='{"id": "123"}',
        key="test-key",
        offset=100,
        partition=0,
    )

    message2 = KafkaMessage(
        topic="different-topic",
        value='{"id": "123"}',
        key="test-key",
        offset=100,
        partition=0,
    )

    assert message1 != message2


def test_producer_config_minimal() -> None:
    """Test creating a minimal KafkaProducerConfig."""
    config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
    )

    assert config.kafka_servers == "localhost:9092"
    assert config.kafka_group_id == "test-group"
    assert config.kafka_topic_prefix is None
    assert config.kafka_block_buffer_time_sec == 10
    assert config.kafka_connection_check_timeout_sec == 5
    assert config.kafka_connection_check_interval_sec == 60


def test_producer_config_with_defaults() -> None:
    """Test creating a KafkaProducerConfig with all defaults."""
    config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_topic_prefix="test.",
        kafka_block_buffer_time_sec=15,
        kafka_connection_check_timeout_sec=10,
        kafka_connection_check_interval_sec=30,
    )

    assert config.kafka_servers == "localhost:9092"
    assert config.kafka_group_id == "test-group"
    assert config.kafka_topic_prefix == "test."
    assert config.kafka_block_buffer_time_sec == 15
    assert config.kafka_connection_check_timeout_sec == 10
    assert config.kafka_connection_check_interval_sec == 30


def test_producer_config_with_custom_defaults() -> None:
    """Test creating a KafkaProducerConfig with custom values."""
    config = KafkaProducerConfig(
        kafka_servers="kafka1:9092,kafka2:9092",
        kafka_group_id="custom-group",
        kafka_topic_prefix="custom.",
        kafka_block_buffer_time_sec=20,
        kafka_connection_check_timeout_sec=15,
        kafka_connection_check_interval_sec=45,
    )

    assert config.kafka_servers == "kafka1:9092,kafka2:9092"
    assert config.kafka_group_id == "custom-group"
    assert config.kafka_topic_prefix == "custom."
    assert config.kafka_block_buffer_time_sec == 20
    assert config.kafka_connection_check_timeout_sec == 15
    assert config.kafka_connection_check_interval_sec == 45


def test_producer_config_with_none_values() -> None:
    """Test creating a KafkaProducerConfig with None values."""
    config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_topic_prefix=None,
    )

    assert config.kafka_topic_prefix is None


def test_producer_config_with_list_servers() -> None:
    """Test creating a KafkaProducerConfig with list of servers."""
    config = KafkaProducerConfig(
        kafka_servers=["localhost:9092", "localhost:9093"],
        kafka_group_id="test-group",
    )

    assert config.kafka_servers == ["localhost:9092", "localhost:9093"]


def test_producer_config_with_empty_list_servers() -> None:
    """Test creating a KafkaProducerConfig with empty server list."""
    with pytest.raises(ValidationError):
        KafkaProducerConfig(
            kafka_servers=[],
            kafka_group_id="test-group",
        )


def test_producer_config_with_empty_string_servers() -> None:
    """Test creating a KafkaProducerConfig with empty string servers."""
    with pytest.raises(ValidationError):
        KafkaProducerConfig(
            kafka_servers="",
            kafka_group_id="test-group",
        )


def test_producer_config_with_special_characters() -> None:
    """Test creating a KafkaProducerConfig with special characters."""
    config = KafkaProducerConfig(
        kafka_servers="kafka-server:9092",
        kafka_group_id="test-group-with-special-chars: @#$%^&*()",
        kafka_topic_prefix="test-prefix-with-special-chars: @#$%^&*().",
    )

    assert config.kafka_group_id == "test-group-with-special-chars: @#$%^&*()"
    assert config.kafka_topic_prefix == "test-prefix-with-special-chars: @#$%^&*()."


def test_producer_config_with_unicode() -> None:
    """Test creating a KafkaProducerConfig with unicode characters."""
    config = KafkaProducerConfig(
        kafka_servers="kafka-server:9092",
        kafka_group_id="test-group-with-unicode-测试",
        kafka_topic_prefix="test-prefix-with-unicode-测试.",
    )

    assert config.kafka_group_id == "test-group-with-unicode-测试"
    assert config.kafka_topic_prefix == "test-prefix-with-unicode-测试."


def test_producer_config_with_zero_timeouts() -> None:
    """Test creating a KafkaProducerConfig with zero timeouts."""
    config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_block_buffer_time_sec=0,
        kafka_connection_check_timeout_sec=0,
        kafka_connection_check_interval_sec=0,
    )

    assert config.kafka_block_buffer_time_sec == 0
    assert config.kafka_connection_check_timeout_sec == 0
    assert config.kafka_connection_check_interval_sec == 0


def test_producer_config_with_negative_timeouts() -> None:
    """Test creating a KafkaProducerConfig with negative timeouts."""
    with pytest.raises(ValidationError):
        KafkaProducerConfig(
            kafka_servers="localhost:9092",
            kafka_group_id="test-group",
            kafka_block_buffer_time_sec=-1,
        )


def test_producer_config_with_large_timeouts() -> None:
    """Test creating a KafkaProducerConfig with large timeouts."""
    config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_block_buffer_time_sec=3600,
        kafka_connection_check_timeout_sec=300,
        kafka_connection_check_interval_sec=1800,
    )

    assert config.kafka_block_buffer_time_sec == 3600
    assert config.kafka_connection_check_timeout_sec == 300
    assert config.kafka_connection_check_interval_sec == 1800


def test_consumer_config_minimal() -> None:
    """Test creating a minimal KafkaConsumerConfig."""
    config = KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
    )

    assert config.kafka_servers == "localhost:9092"
    assert config.kafka_group_id == "test-group"
    assert config.kafka_offset == "latest"
    assert config.kafka_topic_prefix is None
    assert config.kafka_block_consumer_time_sec == 2
    assert config.kafka_connection_check_timeout_sec == 5
    assert config.kafka_auto_commit_interval_ms == 1000
    assert config.kafka_connection_check_interval_sec == 60


def test_consumer_config_with_none_values() -> None:
    """Test creating a KafkaConsumerConfig with None values."""
    config = KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_topic_prefix=None,
    )

    assert config.kafka_topic_prefix is None


def test_consumer_config_with_list_servers() -> None:
    """Test creating a KafkaConsumerConfig with list of servers."""
    config = KafkaConsumerConfig(
        kafka_servers=["localhost:9092", "localhost:9093"],
        kafka_group_id="test-group",
    )

    assert config.kafka_servers == ["localhost:9092", "localhost:9093"]


def test_consumer_config_with_different_offsets() -> None:
    """Test creating a KafkaConsumerConfig with different offset values."""
    configs = [
        KafkaConsumerConfig(
            kafka_servers="localhost:9092",
            kafka_group_id="test-group",
            kafka_offset="earliest",
        ),
        KafkaConsumerConfig(
            kafka_servers="localhost:9092",
            kafka_group_id="test-group",
            kafka_offset="latest",
        ),
    ]

    assert configs[0].kafka_offset == "earliest"
    assert configs[1].kafka_offset == "latest"


def test_consumer_config_with_special_characters() -> None:
    """Test creating a KafkaConsumerConfig with special characters."""
    config = KafkaConsumerConfig(
        kafka_servers="kafka-server:9092",
        kafka_group_id="test-group-with-special-chars: @#$%^&*()",
        kafka_topic_prefix="test-prefix-with-special-chars: @#$%^&*().",
    )

    assert config.kafka_group_id == "test-group-with-special-chars: @#$%^&*()"
    assert config.kafka_topic_prefix == "test-prefix-with-special-chars: @#$%^&*()."


def test_consumer_config_with_unicode() -> None:
    """Test creating a KafkaConsumerConfig with unicode characters."""
    config = KafkaConsumerConfig(
        kafka_servers="kafka-server:9092",
        kafka_group_id="test-group-with-unicode-测试",
        kafka_topic_prefix="test-prefix-with-unicode-测试.",
    )

    assert config.kafka_group_id == "test-group-with-unicode-测试"
    assert config.kafka_topic_prefix == "test-prefix-with-unicode-测试."


def test_consumer_config_with_zero_timeouts() -> None:
    """Test creating a KafkaConsumerConfig with zero timeouts."""
    config = KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_block_consumer_time_sec=0,
        kafka_connection_check_timeout_sec=0,
        kafka_connection_check_interval_sec=0,
        kafka_auto_commit_interval_ms=0,
    )

    assert config.kafka_block_consumer_time_sec == 0
    assert config.kafka_connection_check_timeout_sec == 0
    assert config.kafka_connection_check_interval_sec == 0
    assert config.kafka_auto_commit_interval_ms == 0


def test_consumer_config_with_negative_timeouts() -> None:
    """Test creating a KafkaConsumerConfig with negative timeouts."""
    with pytest.raises(ValidationError):
        KafkaConsumerConfig(
            kafka_servers="localhost:9092",
            kafka_group_id="test-group",
            kafka_block_consumer_time_sec=-1,
        )


def test_consumer_config_with_large_timeouts() -> None:
    """Test creating a KafkaConsumerConfig with large timeouts."""
    config = KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_block_consumer_time_sec=300,
        kafka_connection_check_timeout_sec=60,
        kafka_connection_check_interval_sec=1800,
        kafka_auto_commit_interval_ms=10000,
    )

    assert config.kafka_block_consumer_time_sec == 300
    assert config.kafka_connection_check_timeout_sec == 60
    assert config.kafka_connection_check_interval_sec == 1800
    assert config.kafka_auto_commit_interval_ms == 10000


def test_consumer_config_with_empty_string_offset() -> None:
    """Test creating a KafkaConsumerConfig with empty string offset."""
    with pytest.raises(ValidationError):
        KafkaConsumerConfig(
            kafka_servers="localhost:9092",
            kafka_group_id="test-group",
            kafka_offset="",
        )


def test_producer_config_missing_required_fields() -> None:
    """Test that missing required fields raise ValidationError."""
    with pytest.raises(ValidationError):
        KafkaProducerConfig()  # type: ignore[call-arg]

    with pytest.raises(ValidationError):
        KafkaProducerConfig(kafka_servers="localhost:9092")  # type: ignore[call-arg]

    with pytest.raises(ValidationError):
        KafkaProducerConfig(kafka_group_id="test-group")  # type: ignore[call-arg]


def test_consumer_config_missing_required_fields() -> None:
    """Test that missing required fields raise ValidationError."""
    with pytest.raises(ValidationError):
        KafkaConsumerConfig()  # type: ignore[call-arg]

    with pytest.raises(ValidationError):
        KafkaConsumerConfig(kafka_servers="localhost:9092")  # type: ignore[call-arg]

    with pytest.raises(ValidationError):
        KafkaConsumerConfig(kafka_group_id="test-group")  # type: ignore[call-arg]


def test_kafka_message_missing_required_fields() -> None:
    """Test that missing required fields raise ValidationError."""
    with pytest.raises(ValidationError):
        KafkaMessage()  # type: ignore[call-arg]

    with pytest.raises(ValidationError):
        KafkaMessage(topic="test-topic")  # type: ignore[call-arg]


def test_config_immutability() -> None:
    """Test that config objects are immutable."""
    config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
    )

    # Configs should be frozen (immutable)
    with pytest.raises(FrozenInstanceError):
        config.kafka_group_id = "new-group"  # type: ignore[misc]


def test_config_equality() -> None:
    """Test config equality comparison."""
    config1 = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
    )

    config2 = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
    )

    assert config1 == config2


def test_config_inequality() -> None:
    """Test config inequality comparison."""
    config1 = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
    )

    config2 = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="different-group",
    )

    assert config1 != config2


# SASL Configuration Tests


def test_config_with_sasl_authentication() -> None:
    """Test creating configs with SASL authentication."""
    producer_config = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_security_protocol=KafkaSecurityProtocol.SASL_PLAINTEXT,
        kafka_sasl_mechanism=KafkaSaslMechanism.PLAIN,
        kafka_sasl_username="test-user",
        kafka_sasl_password="test-password",
    )

    consumer_config = KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_security_protocol=KafkaSecurityProtocol.SASL_SSL,
        kafka_sasl_mechanism=KafkaSaslMechanism.SCRAM_SHA_256,
        kafka_sasl_username="test-user",
        kafka_sasl_password="test-password",
    )

    assert (
        producer_config.kafka_security_protocol == KafkaSecurityProtocol.SASL_PLAINTEXT
    )
    assert producer_config.kafka_sasl_mechanism == KafkaSaslMechanism.PLAIN
    assert consumer_config.kafka_security_protocol == KafkaSecurityProtocol.SASL_SSL
    assert consumer_config.kafka_sasl_mechanism == KafkaSaslMechanism.SCRAM_SHA_256


def test_config_invalid_sasl_values() -> None:
    """Test that invalid SASL values raise ValidationError."""
    with pytest.raises(ValidationError):
        KafkaProducerConfig(
            kafka_servers="localhost:9092",
            kafka_group_id="test-group",
            kafka_security_protocol="INVALID",  # type: ignore[arg-type]
        )

    with pytest.raises(ValidationError):
        KafkaConsumerConfig(
            kafka_servers="localhost:9092",
            kafka_group_id="test-group",
            kafka_sasl_mechanism="INVALID",  # type: ignore[arg-type]
        )


def test_to_kafka_config_with_and_without_sasl() -> None:
    """Test that to_kafka_config() correctly includes/excludes SASL parameters."""
    # Without SASL
    config_no_sasl = KafkaProducerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
    )
    kafka_config_no_sasl = config_no_sasl.to_kafka_config()

    assert "security.protocol" not in kafka_config_no_sasl
    assert "sasl.mechanism" not in kafka_config_no_sasl
    assert "sasl.username" not in kafka_config_no_sasl
    assert "sasl.password" not in kafka_config_no_sasl

    # With SASL
    config_with_sasl = KafkaConsumerConfig(
        kafka_servers="localhost:9092",
        kafka_group_id="test-group",
        kafka_security_protocol=KafkaSecurityProtocol.SASL_SSL,
        kafka_sasl_mechanism=KafkaSaslMechanism.SCRAM_SHA_256,
        kafka_sasl_username="user",
        kafka_sasl_password="pass",
    )
    kafka_config_with_sasl = config_with_sasl.to_kafka_config()

    assert kafka_config_with_sasl["security.protocol"] == "SASL_SSL"
    assert kafka_config_with_sasl["sasl.mechanism"] == "SCRAM-SHA-256"
    assert kafka_config_with_sasl["sasl.username"] == "user"
    assert kafka_config_with_sasl["sasl.password"] == "pass"
