import logging
from typing import Tuple
import json

from ..kafka_service.kafka_producer import KafkaProducer  # type: ignore[misc] # pylint: disable=relative-beyond-top-level
from ..kafka_service.schemas import KafkaMessage  # type: ignore[misc] # pylint: disable=relative-beyond-top-level
from .output import (  # pylint: disable=relative-beyond-top-level
    DlqOutputMessagePayload,
    FailedFeatureData,
    FailedMessageData,
)


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

    def handle(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        message: KafkaMessage,
        failures_details: list[FailedFeatureData | FailedMessageData] | None,
    ) -> None:
        """
        Handles a failed message by building a DLQ msg and sending it to the DLQ topic.

        :param message: The original Kafka message.
        :param failed_features: A list of failed features.
        """
        logging.info("Handling message for DLQ.")

        kafka_headers = self._build_headers(
            failures_details=failures_details,
        )

        dlq_message = self._build_message(message)

        logging.info("DLQ message built successfully.")
        self._send_message(dlq_message, kafka_headers)

    def _build_message(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        message: KafkaMessage,
    ) -> DlqOutputMessagePayload:
        """
        Builds a DLQ message payload.

        :param message: The original Kafka message.
        :return: A DlqOutputMessagePayload instance.
        """

        return DlqOutputMessagePayload(message=message.value)

    def _build_headers(
        self,
        failures_details: list[FailedFeatureData | FailedMessageData] | None,
    ) -> list[Tuple[str, bytes]]:
        """
        Builds Kafka headers containing exception details.

        :param failed_features: A list of FailedFeatureData objects.
        :return: A list of Kafka headers.
        """
        headers: list[Tuple[str, bytes]] = []

        if failures_details:
            for index, failure in enumerate(failures_details):
                if isinstance(failure, FailedFeatureData) and failure.failed_feature:
                    headers.append(
                        (
                            f"failed_feature_{index}",
                            failure.failed_feature.encode("utf-8"),
                        )
                    )

                if failure.raised_at:
                    headers.append(
                        (f"raised_at_{index}", failure.raised_at.encode("utf-8"))
                    )

                if failure.exception_message:
                    headers.append(
                        (
                            f"exception_message_{index}",
                            failure.exception_message.encode("utf-8"),
                        )
                    )

                if failure.exception_trace:
                    headers.append(
                        (
                            f"exception_trace_{index}",
                            json.dumps(failure.exception_trace).encode("utf-8"),
                        )
                    )

        return headers

    def _send_message(
        self, message: DlqOutputMessagePayload, headers: list[tuple[str, bytes]]
    ) -> None:
        """
        Sends a DLQ message to the Kafka DLQ topic with headers.

        :param message: The DLQ message payload.
        :param headers: Kafka headers containing exception details.
        """
        serialized_message = message.model_dump_json()
        self.producer.send(
            topic=self.dlq_topic, value=serialized_message, key=None, headers=headers
        )
        logging.info("Message sent to DLQ topic successfully with headers: %s", headers)
