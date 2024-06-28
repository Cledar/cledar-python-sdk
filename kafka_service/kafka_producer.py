from confluent_kafka import Producer, KafkaException
from settings import Settings
from .utils import build_topic, delivery_callback
from .logger import logger
from .exceptions import KafkaConnectionError, KafkaProducerNotConnectedError


class KafkaProducer:
    def __init__(self, config: Settings) -> None:
        self.config = config
        logger.info("Initializing KafkaProducer.", extra={"config": config})
        self.producer = None

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

    def send(self, topic: str, key: str, value: str) -> None:
        if self.producer is None:
            logger.error(
                "KafkaProducer is not connected. Call 'connect' first.",
                extra={"topic": topic, "key": key, "value": value},
            )
            raise KafkaProducerNotConnectedError

        topic = build_topic(topic_name=topic, prefix=self.config.kafka_topic_prefix)

        try:
            logger.info(
                "Sending message to topic.",
                extra={"topic": topic, "key": key, "value": value},
            )
            self.producer.produce(topic, key, value, callback=delivery_callback)
            self.producer.poll(0)

        except BufferError:
            logger.warning("Buffer full, waiting for free space on the queue")
            self.producer.poll(self.config.kafka_block_buffer_time)
            self.producer.produce(topic, key, value, callback=delivery_callback)

        except KafkaException as exception:
            logger.error(
                "Failed to send message.",
                extra={"exception": str(exception)},
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
            self.producer.list_topics(timeout=1)
        except KafkaException as exception:
            logger.error(
                "Failed to connect to Kafka servers.",
                extra={"exception": str(exception)},
            )
            raise KafkaConnectionError from exception

    def prepare_for_shutdown(self) -> None:
        logger.info("Flushing KafkaProducer.")
        self.producer.flush(-1)
        logger.info("KafkaProducer flushed.")


def create_producer(config: Settings) -> KafkaProducer:
    return KafkaProducer(config=config)
