import json

from pydantic import ConfigDict
from pydantic.dataclasses import dataclass
from confluent_kafka import Consumer, KafkaException

from .base_kafka_client import BaseKafkaClient
from .schemas import KafkaConsumerConfig
from .utils import build_topic, consumer_not_connected_msg
from .schemas import KafkaMessage
from .logger import logger
from .exceptions import (
    KafkaConsumerNotConnectedError,
    KafkaConsumerError,
)


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class KafkaConsumer(BaseKafkaClient):
    config: KafkaConsumerConfig
    client: Consumer | None = None

    def connect(self) -> None:
        self.client = Consumer(
            {
                "bootstrap.servers": self.config.kafka_servers,
                "enable.auto.commit": False,
                "enable.partition.eof": False,
                "auto.commit.interval.ms": self.config.kafka_auto_commit_interval_ms,
                "auto.offset.reset": self.config.kafka_offset,
                "group.id": self.config.kafka_group_id,
            }
        )
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
            msg_id = (
                json.loads(msg.value().decode("utf-8")).get("id", None)
                if msg.value()
                else None
            )
            logger.debug(
                "Received message.",
                extra={
                    "topic": msg.topic(),
                    "msg_id": msg_id,
                    "key": msg.key(),
                },
            )
            return KafkaMessage(
                topic=msg.topic(),
                value=msg.value().decode("utf-8") if msg.value() else None,
                key=msg.key().decode("utf-8") if msg.key() else None,
                offset=msg.offset(),
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
            logger.debug("Commit requested.", extra={"offset": message.offset})

        except KafkaException as exception:
            logger.exception("Failed to commit offsets.")
            raise exception
