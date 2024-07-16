import threading
import time
from confluent_kafka import Producer, KafkaException
from stream_chunker.settings import Settings
from .utils import build_topic, delivery_callback
from .logger import logger
from .exceptions import KafkaConnectionError, KafkaProducerNotConnectedError


class KafkaProducer:
    def __init__(self, config: Settings) -> None:
        self.config: Settings = config
        logger.info("Initializing KafkaProducer.", extra={"config": config})
        self.producer: Producer | None = None
        self.connection_check_thread: threading.Thread | None = None
        self._stop_event: threading.Event = threading.Event()

    def connect(self) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": self.config.kafka_servers,
                "client.id": self.config.kafka_group_id,
            }
        )
        self.check_connection()
        logger.info(
            "Connected to Kafka servers.",
            extra={"kafka_servers": self.config.kafka_servers},
        )
        self.start_connection_check_thread()

    def start_connection_check_thread(self) -> None:
        if self.connection_check_thread is None:
            self.connection_check_thread = threading.Thread(
                target=self._monitor_connection
            )
            self.connection_check_thread.start()
            logger.info(
                "Started kafka connection check thread.",
                extra={"interval": self.config.kafka_connection_check_interval_sec},
            )

    def _monitor_connection(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.check_connection()
                logger.info("Kafka connection status: Connected.")
            except KafkaConnectionError as exception:
                logger.error(
                    "Kafka connection check failed.",
                    extra={"exception": str(exception)},
                )
            time.sleep(self.config.kafka_connection_check_interval_sec)

    def send(self, topic: str, value: str, key: str) -> None:
        if self.producer is None:
            logger.error(
                "KafkaProducer is not connected. Call 'connect' first.",
                extra={"topic": topic, "value": value, "key": key},
            )
            raise KafkaProducerNotConnectedError

        topic = build_topic(topic_name=topic, prefix=self.config.kafka_topic_prefix)

        try:
            logger.debug(
                "Sending message to topic.",
                extra={"topic": topic, "value": value, "key": key},
            )
            self.producer.produce(
                topic=topic, value=value, key=key, callback=delivery_callback
            )
            self.producer.poll(0)
            logger.debug(
                "Sent message to topic.",
                extra={"topic": topic, "value": value, "key": key},
            )

        except BufferError:
            logger.warning(
                "KafkaProducer buffer is full, waiting for buffer to empty.",
                extra={"topic": topic},
            )
            self.producer.poll(self.config.kafka_block_buffer_time_sec)
            self.producer.produce(
                topic=topic, value=value, key=key, callback=delivery_callback
            )

        except KafkaException as exception:
            logger.error(
                "Failed to send message.",
                extra={"exception": str(exception), "topic": topic},
            )
            raise exception

    def check_connection(self) -> None:
        """
        when the broker is not available (or the address is wrong)
        the 'connection refused' error is not catched
        https://github.com/confluentinc/confluent-kafka-python/issues/941
        the below is far-from-perfect workaround handling that
        """
        try:
            self.producer.list_topics(
                timeout=self.config.kafka_connection_check_timeout_sec
            )
        except KafkaException as exception:
            logger.error(
                "Failed to connect to Kafka servers.",
                extra={"exception": str(exception)},
            )
            raise KafkaConnectionError from exception

    def shutdown(self) -> None:
        logger.info("Shutting down KafkaProducer.")
        self._stop_event.set()
        if self.connection_check_thread is not None:
            self.connection_check_thread.join()  # Wait for the thread to finish
        logger.info("Flushing KafkaProducer.")
        self.producer.flush(-1)
        logger.info("KafkaProducer flushed.")


def create_producer(config: Settings) -> KafkaProducer:
    return KafkaProducer(config=config)
