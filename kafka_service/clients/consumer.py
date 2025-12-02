from confluent_kafka import Consumer, KafkaException
from pydantic import ConfigDict
from pydantic.dataclasses import dataclass

from ..config.schemas import KafkaConsumerConfig
from ..exceptions import (
    KafkaConsumerError,
    KafkaConsumerNotConnectedError,
)
from ..logger import logger
from ..models.message import KafkaMessage
from ..utils.messages import consumer_not_connected_msg, extract_id_from_value
from ..utils.topics import build_topic
from .base import BaseKafkaClient


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class KafkaConsumer(BaseKafkaClient):
    config: KafkaConsumerConfig
    client: Consumer | None = None

    def connect(self) -> None:
        self.client = Consumer(self.config.to_kafka_config())
        self.check_connection()
        logger.info(
            "Connected KafkaConsumer to Kafka servers.",
            extra={"kafka_servers": self.config.kafka_servers},
        )
        self.start_connection_check_thread()

    def subscribe(self, topics: list[str]) -> None:
        if self.client is None:
            logger.error(
                consumer_not_connected_msg,
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
            self.client.subscribe(topics)

        except KafkaException as exception:
            logger.exception(
                "Failed to subscribe to topics.",
                extra={"topics": topics},
            )
            raise exception

    def consume_next(self) -> KafkaMessage | None:
        if self.client is None:
            logger.error(consumer_not_connected_msg)
            raise KafkaConsumerNotConnectedError

        try:
            msg = self.client.poll(self.config.kafka_block_consumer_time_sec)

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
                    "msg_id": extract_id_from_value(msg.value().decode("utf-8")),
                    "key": msg.key(),
                },
            )
            return KafkaMessage(
                topic=msg.topic(),
                value=msg.value().decode("utf-8") if msg.value() else None,
                key=msg.key().decode("utf-8") if msg.key() else None,
                offset=msg.offset(),
                partition=msg.partition(),
            )

        except KafkaException as exception:
            logger.exception("Failed to consume message.")
            raise exception

    def commit(self, message: KafkaMessage) -> None:
        if self.client is None:
            logger.error(consumer_not_connected_msg)
            raise KafkaConsumerNotConnectedError

        try:
            self.client.commit(asynchronous=True)
            logger.debug(
                "Commit requested.",
                extra={"offset": message.offset, "partition": message.partition},
            )

        except KafkaException as exception:
            logger.exception("Failed to commit offsets.")
            raise exception
