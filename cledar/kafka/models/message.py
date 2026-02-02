"""Kafka message data class."""

from pydantic.dataclasses import dataclass


@dataclass
class KafkaMessage:
    """Base Kafka message representation."""

    topic: str
    value: str | None
    key: str | None
    offset: int | None
    partition: int | None
