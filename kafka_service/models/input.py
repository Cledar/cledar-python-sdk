import dataclasses

from pydantic import BaseModel
from ..schemas import KafkaMessage  # pylint: disable=relative-beyond-top-level


@dataclasses.dataclass
class InputKafkaMessage[Payload: BaseModel](KafkaMessage):
    payload: Payload
