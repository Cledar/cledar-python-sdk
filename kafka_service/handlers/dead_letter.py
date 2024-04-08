import json

from ..clients.producer import KafkaProducer
from ..logger import logger
from ..models.message import KafkaMessage
from ..models.output import FailedMessageData


class DeadLetterHandler:
    """
    A Handler for handling failed messages and sending them to a DLQ topic.
    """

    def __init__(self, producer: KafkaProducer, dlq_topic: str) -> None:
        """
        Initialize DeadLetterHandler with a Kafka producer and DLQ topic.

        :param producer: KafkaProducer instance.
        :param dlq_topic: The name of the DLQ Kafka topic.
        """
        self.producer: KafkaProducer = producer
        self.dlq_topic: str = dlq_topic

    def handle(
        self,
        message: KafkaMessage,
        failures_details: list[FailedMessageData] | None,
    ) -> None:
        """
        Handles a failed message by sending it to the DLQ topic.

        :param message: The original Kafka message.
        :param failures_details: A list of FailedMessageData.
        """
        logger.info("Handling message for DLQ.")

        kafka_headers = self._build_headers(failures_details=failures_details)

        logger.info("DLQ message built successfully.")
        self._send_message(message.value, message.key, kafka_headers)

    def _build_headers(
        self,
        failures_details: list[FailedMessageData] | None,
    ) -> list[tuple[str, bytes]]:
        """
        Builds Kafka headers containing exception details.

        :param failures_details: A list of FailedMessageData.
        :return: A list of Kafka headers.
        """
        headers: list[tuple[str, bytes]] = []

        if failures_details:
            failures_json = json.dumps(
                [failure.model_dump() for failure in failures_details]
            )
            headers.append(("failures_details", failures_json.encode("utf-8")))

        return headers

    def _send_message(
        self,
        message: str | None,
        key: str | None,
        headers: list[tuple[str, bytes]],
    ) -> None:
        """
        Sends a DLQ message to the Kafka DLQ topic with headers.

        :param message: The DLQ message payload.
        :param key: The original Kafka message key.
        :param headers: Kafka headers containing exception details.
        """
        self.producer.send(
            topic=self.dlq_topic, value=message, key=key, headers=headers
        )
        logger.info(
            "Message sent to DLQ topic successfully with key: %s and headers: %s",
            key,
            headers,
        )
