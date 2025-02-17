from pydantic.dataclasses import dataclass


@dataclass
class KafkaMessage:
    topic: str
    value: str | None
    key: str | None
    offset: int | None
    partition: int | None


@dataclass
class KafkaProducerConfig:
    # pylint: disable=too-many-instance-attributes
    kafka_servers: list[str] | str
    kafka_group_id: str | None
    kafka_topic_prefix: str | None
    kafka_block_buffer_time_sec: int
    kafka_connection_check_timeout_sec: int
    kafka_connection_check_interval_sec: int
    kafka_partitioner: str = "consistent_random"
    compression_type: str | None = "gzip"


@dataclass
class KafkaConsumerConfig:
    # pylint: disable=too-many-instance-attributes
    kafka_servers: list[str] | str
    kafka_group_id: str | None
    kafka_offset: str
    kafka_topic_prefix: str | None
    kafka_block_consumer_time_sec: int
    kafka_connection_check_timeout_sec: int
    kafka_auto_commit_interval_ms: int
    kafka_connection_check_interval_sec: int
