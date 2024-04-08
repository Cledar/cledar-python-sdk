from pydantic import field_validator
from pydantic.dataclasses import dataclass


def _validate_kafka_servers(v: list[str] | str) -> list[str] | str:
    """Validate kafka_servers is not empty."""
    if isinstance(v, str) and v.strip() == "":
        raise ValueError("kafka_servers cannot be empty")
    if isinstance(v, list) and len(v) == 0:
        raise ValueError("kafka_servers cannot be empty list")
    return v


def _validate_non_negative(v: int) -> int:
    """Validate that timeout values are non-negative."""
    if v < 0:
        raise ValueError("timeout values must be non-negative")
    return v


@dataclass(frozen=True)
class KafkaProducerConfig:
    """Configuration for Kafka Producer.

    Args:
        kafka_servers: List of Kafka broker addresses or comma-separated string
        kafka_group_id: Consumer group identifier
        kafka_topic_prefix: Optional prefix for topic names
        kafka_block_buffer_time_sec: Time to block when buffer is full
        kafka_connection_check_timeout_sec: Timeout for connection health checks
        kafka_connection_check_interval_sec: Interval between connection checks
        kafka_partitioner: Partitioning strategy for messages
        compression_type: Compression type for messages (gzip, snappy, lz4, zstd,
            or None)
    """

    kafka_servers: list[str] | str
    kafka_group_id: str
    kafka_topic_prefix: str | None = None
    kafka_block_buffer_time_sec: int = 10
    kafka_connection_check_timeout_sec: int = 5
    kafka_connection_check_interval_sec: int = 60
    kafka_partitioner: str = "consistent_random"
    compression_type: str | None = "gzip"

    @field_validator("kafka_servers")
    @classmethod
    def validate_kafka_servers(cls, v: list[str] | str) -> list[str] | str:
        return _validate_kafka_servers(v)

    @field_validator(
        "kafka_block_buffer_time_sec",
        "kafka_connection_check_timeout_sec",
        "kafka_connection_check_interval_sec",
    )
    @classmethod
    def validate_positive_timeouts(cls, v: int) -> int:
        return _validate_non_negative(v)


@dataclass(frozen=True)
class KafkaConsumerConfig:
    """Configuration for Kafka Consumer.

    Args:
        kafka_servers: List of Kafka broker addresses or comma-separated string
        kafka_group_id: Consumer group identifier
        kafka_offset: Starting offset position ('earliest', 'latest', or specific
            offset)
        kafka_topic_prefix: Optional prefix for topic names
        kafka_block_consumer_time_sec: Time to block waiting for messages
        kafka_connection_check_timeout_sec: Timeout for connection health checks
        kafka_auto_commit_interval_ms: Interval for automatic offset commits
        kafka_connection_check_interval_sec: Interval between connection checks
    """

    kafka_servers: list[str] | str
    kafka_group_id: str
    kafka_offset: str = "latest"
    kafka_topic_prefix: str | None = None
    kafka_block_consumer_time_sec: int = 2
    kafka_connection_check_timeout_sec: int = 5
    kafka_auto_commit_interval_ms: int = 1000
    kafka_connection_check_interval_sec: int = 60

    @field_validator("kafka_servers")
    @classmethod
    def validate_kafka_servers(cls, v: list[str] | str) -> list[str] | str:
        return _validate_kafka_servers(v)

    @field_validator("kafka_offset")
    @classmethod
    def validate_kafka_offset(cls, v: str) -> str:
        if v.strip() == "":
            raise ValueError("kafka_offset cannot be empty")
        return v

    @field_validator(
        "kafka_block_consumer_time_sec",
        "kafka_connection_check_timeout_sec",
        "kafka_auto_commit_interval_ms",
        "kafka_connection_check_interval_sec",
    )
    @classmethod
    def validate_positive_timeouts(cls, v: int) -> int:
        return _validate_non_negative(v)
