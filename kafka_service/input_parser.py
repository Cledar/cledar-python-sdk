from typing import Generic

from .schemas import KafkaMessage
from .models.input import (
    Payload,
    InputKafkaMessage,
)


class IncorrectMessageValue(Exception):
    """
    Message needs to have `value` field present in order to be parsed.
    This is unless `model` is set to `None`.
    """


class InputParser(Generic[Payload]):
    def __init__(self, model: type[Payload]) -> None:
        self.model: type[Payload] = model

    def parse_json(self, json: str) -> Payload:
        obj = self.model.model_validate_json(json)
        return obj

    def parse_message(self, message: KafkaMessage) -> InputKafkaMessage[Payload]:
        if message.value is None and self.model is not None:
            raise IncorrectMessageValue

        obj = self.parse_json(message.value)

        return InputKafkaMessage(
            key=message.key,
            value=message.value,
            payload=obj,
            topic=message.topic,
        )
