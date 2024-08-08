from pydantic.dataclasses import dataclass


@dataclass
class KafkaMessage:
    topic: str
    value: str | None
    key: str | None


@dataclass
class KafkaProducerConfig:
    kafka_servers: list[str] | str
    kafka_group_id: str | None
    kafka_topic_prefix: str | None
    kafka_block_buffer_time_sec: int
    kafka_connection_check_timeout_sec: int
    kafka_connection_check_interval_sec: int
