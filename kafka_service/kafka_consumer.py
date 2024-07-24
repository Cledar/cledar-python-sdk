from pydantic import ConfigDict
from pydantic.dataclasses import dataclass
from confluent_kafka import Consumer, KafkaException
from .schemas import KafkaConsumerSettings
from .utils import build_topic
from .schemas import KafkaMessage
from .logger import logger
from .exceptions import (
    KafkaConnectionError,
    KafkaConsumerNotConnectedError,
    KafkaConsumerError,
)


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class KafkaConsumer:
    config: KafkaConsumerSettings
    consumer: Consumer | None = None

    def __post_init__(self) -> None:
        logger.info("Initializing KafkaConsumer.", extra={"config": self.config})

    def connect(self) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.config.kafka_servers,
                "enable.auto.commit": True,
                "enable.partition.eof": False,
                "auto.commit.interval.ms": self.config.kafka_auto_commit_interval_ms,
                "auto.offset.reset": self.config.kafka_reference_chunks_offset,
                "group.id": self.config.kafka_group_id,
            }
        )
        self.check_connection()
        logger.info(
            "Connected to Kafka servers.",
            extra={"kafka_servers": self.config.kafka_servers},
        )

    def subscribe(self, topics: list[str]) -> None:
        if self.consumer is None:
            logger.error(
                "KafkaConsumer is not connected. Call 'connect' first.",
                extra={"topics": topics},
            )
            raise KafkaConsumerNotConnectedError

        topics = [
            build_topic(topic_name=topic, prefix=self.config.kafka_topic_prefix)
            for topic in topics
        ]

        try:
            logger.info(
                "Subscribing to topics.",
                extra={"topics": topics},
            )
            self.consumer.subscribe(topics)

        except KafkaException as exception:
            logger.error(
                "Failed to subscribe to topics.",
                extra={"exception": str(exception), "topics": topics},
            )
            raise exception

    def consume_next(self) -> KafkaMessage | None:
        if self.consumer is None:
            logger.error("KafkaConsumer is not connected. Call 'connect' first.")
            raise KafkaConsumerNotConnectedError

        try:
            msg = self.consumer.poll(self.config.kafka_block_consumer_time_sec)

            if msg is None:
                return None

            if msg.error():
                logger.error(
                    "Consumer error.",
                    extra={"error": msg.error()},
                )
                raise KafkaConsumerError(msg.error())

            logger.debug(
                "Received message.",
                extra={
                    "topic": msg.topic(),
                    "value": msg.value(),
                    "key": msg.key(),
                },
            )
            return KafkaMessage(
                topic=msg.topic(),
                value=msg.value().decode("utf-8") if msg.value() else None,
                key=msg.key().decode("utf-8") if msg.key() else None,
            )

        except KafkaException as exception:
            logger.error(
                "Failed to consume message.",
                extra={"exception": str(exception)},
            )
            raise exception

    def check_connection(self) -> None:
        """
        when the broker is not available (or the address is wrong)
        the 'connection refused' error is not caught
        https://github.com/confluentinc/confluent-kafka-python/issues/941
        the below is far-from-perfect workaround handling that
        """
        try:
            self.consumer.list_topics(
                timeout=self.config.kafka_connection_check_timeout_sec
            )
        except KafkaException as exception:
            logger.error(
                "Failed to connect to Kafka servers.",
                extra={"exception": str(exception)},
            )
            raise KafkaConnectionError from exception

    def shutdown(self) -> None:
        logger.info("Closing KafkaConsumer.")
        self.consumer.close()
        logger.info("KafkaConsumer closed.")
