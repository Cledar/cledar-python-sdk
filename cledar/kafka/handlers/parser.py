"""Kafka message parser module."""

import json

from pydantic import BaseModel

from ..models.input import (
    InputKafkaMessage,
)
from ..models.message import KafkaMessage


class IncorrectMessageValueError(Exception):
    """Message needs to have `value` field present in order to be parsed.

    This is unless `model` is set to `None`.
    """


class InputParser[Payload: BaseModel]:
    """Parser for Kafka messages into Pydantic models.

    Generic class for parsing Kafka messages.
    """

    def __init__(self, model: type[Payload]) -> None:
        """Initialize InputParser with a Pydantic model.

        Args:
            model: The Pydantic model to validate messages against.

        """
        self.model: type[Payload] = model

    def parse_json(self, json_str: str) -> Payload:
        """Parse JSON text and validate into the target Payload model.

        Invalid JSON should raise IncorrectMessageValueError, while schema
        validation errors should bubble up as ValidationError.

        Args:
            json_str: The JSON string to parse.

        Returns:
            Payload: The validated Pydantic model instance.

        Raises:
            IncorrectMessageValueError: If the JSON is invalid.

        """
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            # Normalize invalid JSON into our domain-specific error
            raise IncorrectMessageValueError from exc
        return self.model.model_validate(data)

    def parse_message(self, message: KafkaMessage) -> InputKafkaMessage[Payload]:
        """Parse a Kafka message into an InputKafkaMessage with a validated payload.

        Args:
            message: The Kafka message to parse.

        Returns:
            InputKafkaMessage[Payload]: The parsed message with payload.

        Raises:
            IncorrectMessageValueError: If message value is missing but required.

        """
        if message.value is None and self.model is not None:
            raise IncorrectMessageValueError

        obj = self.parse_json(message.value)

        return InputKafkaMessage(
            key=message.key,
            value=message.value,
            payload=obj,
            topic=message.topic,
            offset=message.offset,
            partition=message.partition,
        )
