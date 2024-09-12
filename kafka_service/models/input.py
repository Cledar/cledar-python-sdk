import dataclasses
from typing import Generic, TypeVar

from pydantic import BaseModel
from ..schemas import KafkaMessage  # pylint: disable=relative-beyond-top-level

Payload = TypeVar("Payload", bound=BaseModel)


@dataclasses.dataclass
class InputKafkaMessage(Generic[Payload], KafkaMessage):
    payload: Payload
