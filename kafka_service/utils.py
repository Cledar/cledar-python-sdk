from confluent_kafka import KafkaError, Message
from .logger import logger


def build_topic(topic_name: str, prefix: str | None) -> str:
    return prefix + topic_name if prefix else topic_name


def delivery_callback(error: KafkaError, msg: Message) -> None:
    if error:
        logger.error(
            "Message failed delivery.", extra={"error": error, "topic": msg.topic()}
        )
    else:
        logger.info("Message delivered.", extra={"topic": msg.topic()})
