"""Base Kafka client module."""

import threading

from confluent_kafka import Consumer, KafkaException, Producer
from pydantic import ConfigDict
from pydantic.dataclasses import dataclass

from ..config.schemas import KafkaConsumerConfig, KafkaProducerConfig
from ..exceptions import (
    KafkaConnectionError,
    KafkaConsumerNotConnectedError,
    KafkaProducerNotConnectedError,
)
from ..logger import logger


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class BaseKafkaClient:
    """Base class for Kafka clients.

    This class provides common functionality for both producers and consumers,
    such as connection monitoring and shutdown handling.
    """

    config: KafkaProducerConfig | KafkaConsumerConfig
    client: Producer | Consumer | None = None
    connection_check_thread: threading.Thread | None = None

    def __post_init__(self) -> None:
        """Initialize the client with instance-level events."""
        # Create instance-level stop event
        self._stop_event = threading.Event()
        logger.info(
            f"Initializing {self.__class__.__name__}.", extra={"config": self.config}
        )

    def start_connection_check_thread(self) -> None:
        """Start a background thread to monitor the Kafka connection."""
        if self.connection_check_thread is None:
            self.connection_check_thread = threading.Thread(
                target=self._monitor_connection
            )
            self.connection_check_thread.start()
            logger.info(
                f"Started {self.__class__.__name__} connection check thread.",
                extra={"interval": self.config.kafka_connection_check_interval_sec},
            )

    def _monitor_connection(self) -> None:
        while not self._stop_event.wait(
            self.config.kafka_connection_check_interval_sec
        ):
            try:
                self.check_connection()
                logger.info(f"{self.__class__.__name__} connection status: Connected.")
            except KafkaConnectionError:
                logger.exception(f"{self.__class__.__name__} connection check failed.")

    def is_alive(self) -> bool:
        """Check if the client is currently connected and alive.

        Returns:
            bool: True if connected, False otherwise.

        """
        try:
            self.check_connection()
            return True
        except (
            KafkaProducerNotConnectedError,
            KafkaConsumerNotConnectedError,
            KafkaConnectionError,
        ):
            return False

    def check_connection(self) -> None:
        """Check the connection to Kafka servers.

        When the broker is not available (or the address is wrong),
        the 'connection refused' error is not caught.
        Ref: https://github.com/confluentinc/confluent-kafka-python/issues/941
        The below is far-from-perfect workaround handling that.

        """
        if self.client is None:
            logger.error(
                f"{self.__class__.__name__} is not connected. Call 'connect' first.",
            )
            raise (
                KafkaProducerNotConnectedError
                if isinstance(self.config, KafkaProducerConfig)
                else KafkaConsumerNotConnectedError
            )
        try:
            self.client.list_topics(
                timeout=self.config.kafka_connection_check_timeout_sec
            )
        except KafkaException as exception:
            logger.exception("Failed to connect to Kafka servers.")
            raise KafkaConnectionError from exception

    def shutdown(self) -> None:
        """Shutdown the client and stop connection monitoring."""
        logger.info("Closing %s...", self.__class__.__name__)
        self._stop_event.set()
        if self.connection_check_thread is not None:
            self.connection_check_thread.join()
            logger.info("Stopped connection check thread.")
        if isinstance(self.client, Producer):
            self.client.flush(-1)
            logger.info("%s flushed.", self.__class__.__name__)
        elif isinstance(self.client, Consumer):
            self.client.close()
        # Clear the client reference to indicate shutdown
        self.client = None
        logger.info("%s closed.", self.__class__.__name__)
