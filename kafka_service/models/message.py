from pydantic.dataclasses import dataclass


@dataclass
class KafkaMessage:
    topic: str
    value: str | None
    key: str | None
    offset: int | None
    partition: int | None
