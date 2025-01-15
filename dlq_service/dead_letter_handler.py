import logging
import json
from datetime import datetime

from ..kafka_service.kafka_producer import (
    KafkaProducer,
)
from ..kafka_service.schemas import (
    KafkaMessage,
)
from .output import (
    DlqOutputMessagePayload,
    FailedMessageData,
)


class DeadLetterHandler:
    """
    A coordinator for handling failed messages and sending them to a DLQ topic.
    """

    def __init__(self, producer: KafkaProducer, dlq_topic: str) -> None:
        """
        Initialize DlqCoordinator with a Kafka producer and DLQ topic.

        :param producer: KafkaProducer instance.
        :param dlq_topic: The name of the DLQ Kafka topic.
        """
        self.producer: KafkaProducer = producer
        self.dlq_topic: str = dlq_topic

    def handle(
        self,
        message: KafkaMessage,
        exception_message: str,
        exception_traceback: str,
        raised_at: datetime,
    ) -> None:
        """
        Handles a failed message by building a DLQ msg and sending it to the DLQ topic.

        :param message: The original Kafka message.
        :param exception_message: The error message describing the failure.
        :param exception_traceback: The stack trace of the exception.
        :param raised_at: The datetime when the exception occurred.
        """
        logging.info("Handling message for DLQ.")
        dlq_message = self._build_message(
            message, exception_message, exception_traceback, raised_at
        )
        logging.info("DLQ message built successfully.")
        self._send_message(dlq_message)

    def _build_message(
        self,
        message: KafkaMessage,
        exception_message: str,
        exception_traceback: str,
        raised_at: datetime,
    ) -> DlqOutputMessagePayload:
        """
        Builds a DLQ message payload.

        :param message: The original Kafka message.
        :param exception_message: The error message.
        :param exception_traceback: The stack trace of the exception.
        :param raised_at: The datetime when the exception occurred.
        :return: A DlqOutputMessagePayload instance.
        """

        failed_message_data = FailedMessageData(
            raised_at=str(raised_at),
            exception_message=str(exception_message),
            exception_trace=exception_traceback,
        )

        return DlqOutputMessagePayload(
            message=message.value, failure=[failed_message_data]
        )

    def _send_message(self, message: DlqOutputMessagePayload) -> None:
        """
        Sends a DLQ message to the Kafka DLQ topic.

        :param message: The DLQ message payload.
        """
        serialized_message = message.model_dump_json()
        json.loads(serialized_message)
        self.producer.send(topic=self.dlq_topic, value=serialized_message, key=None)
        logging.info("Message sent to DLQ topic successfully.")
