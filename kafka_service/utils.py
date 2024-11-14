import json

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
        logger.debug("Message delivered.", extra={"topic": msg.topic()})


_UNKNOWN_ID_PLACEHOLDER = "<unknown_id>"
_ID_FIELD_KEY = "id"


def extract_id_from_value(value: str | None) -> str:
    msg_id: str = _UNKNOWN_ID_PLACEHOLDER
    if value is None:
        return msg_id

    try:
        msg_id = json.loads(value).get(_ID_FIELD_KEY, _UNKNOWN_ID_PLACEHOLDER)
    except json.JSONDecodeError as e:
        logger.error(f"Decoding for id failed. {e.msg}")
    return msg_id


consumer_not_connected_msg = "KafkaConsumer is not connected. Call 'connect' first."
