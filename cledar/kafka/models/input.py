import dataclasses
from typing import TypeVar

from pydantic import BaseModel

from .message import KafkaMessage

Payload = TypeVar("Payload", bound=BaseModel)


@dataclasses.dataclass
class InputKafkaMessage[Payload](KafkaMessage):
    payload: Payload
