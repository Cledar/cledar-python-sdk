import threading
from pydantic import ConfigDict
from pydantic.dataclasses import dataclass
from confluent_kafka import Producer, KafkaException
from .schemas import KafkaProducerConfig
from .logger import logger
from .exceptions import (
    KafkaConnectionError,
)


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class BaseKafkaClient:
    config: KafkaProducerConfig
    client: Producer | None = None
    connection_check_thread: threading.Thread | None = None
    _stop_event: threading.Event = threading.Event()

    def __post_init__(self) -> None:
        logger.info(
            f"Initializing {self.__class__.__name__}.", extra={"config": self.config}
        )

    def start_connection_check_thread(self) -> None:
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

    def check_connection(self) -> None:
        """
        when the broker is not available (or the address is wrong)
        the 'connection refused' error is not caught
        https://github.com/confluentinc/confluent-kafka-python/issues/941
        the below is far-from-perfect workaround handling that
        """
        try:
            self.client.list_topics(
                timeout=self.config.kafka_connection_check_timeout_sec
            )
        except KafkaException as exception:
            logger.exception("Failed to connect to Kafka servers.")
            raise KafkaConnectionError from exception

    def shutdown(self) -> None:
        self._stop_event.set()
        if self.connection_check_thread is not None:
            self.connection_check_thread.join()
        logger.info(f"Closing {self.__class__.__name__}.")
        if isinstance(self.client, Producer):
            self.client.flush(-1)
        logger.info(f"{self.__class__.__name__} closed.")
