"""Input Kafka message model."""

import dataclasses
from typing import TypeVar

from pydantic import BaseModel

from .message import KafkaMessage

Payload = TypeVar("Payload", bound=BaseModel)


@dataclasses.dataclass
class InputKafkaMessage[Payload](KafkaMessage):
    """Kafka message with a parsed and validated Pydantic payload."""

    payload: Payload
