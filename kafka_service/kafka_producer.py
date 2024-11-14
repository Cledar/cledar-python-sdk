from pydantic import ConfigDict
from pydantic.dataclasses import dataclass
from confluent_kafka import Producer, KafkaException

from .base_kafka_client import BaseKafkaClient
from .schemas import KafkaProducerConfig
from .utils import build_topic, delivery_callback, extract_id_from_value
from .logger import logger
from .exceptions import KafkaProducerNotConnectedError


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class KafkaProducer(BaseKafkaClient):
    config: KafkaProducerConfig
    client: Producer | None = None

    def connect(self) -> None:
        self.client = Producer(
            {
                "bootstrap.servers": self.config.kafka_servers,
                "client.id": self.config.kafka_group_id,
                "compression.type": self.config.compression_type,
            }
        )
        self.check_connection()
        logger.info(
            "Connected Producer to Kafka servers.",
            extra={"kafka_servers": self.config.kafka_servers},
        )
        self.start_connection_check_thread()

    def send(self, topic: str, value: str | None, key: str | None) -> None:
        if self.client is None:
            logger.error(
                "KafkaProducer is not connected. Call 'connect' first.",
                extra={
                    "topic": topic,
                    "msg_id": extract_id_from_value(value),
                    "key": key,
                },
            )
            raise KafkaProducerNotConnectedError

        topic = build_topic(topic_name=topic, prefix=self.config.kafka_topic_prefix)

        try:
            logger.debug(
                "Sending message to topic.",
                extra={
                    "topic": topic,
                    "msg_id": extract_id_from_value(value),
                    "key": key,
                },
            )
            self.client.produce(
                topic=topic, value=value, key=key, callback=delivery_callback
            )
            self.client.poll(0)

        except BufferError:
            logger.warning("Buffer full, waiting for free space on the queue")
            self.client.poll(self.config.kafka_block_buffer_time_sec)
            self.client.produce(
                topic=topic, value=value, key=key, callback=delivery_callback
            )

        except KafkaException as exception:
            logger.exception("Failed to send message.")
            raise exception
