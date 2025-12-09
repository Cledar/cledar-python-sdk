from confluent_kafka import KafkaError, Message

from ..logger import logger


def delivery_callback(error: KafkaError, msg: Message) -> None:
    try:
        if msg is None:
            logger.warning("Callback received a None message.")
        else:
            topic = msg.topic()
    except Exception as exc:
        logger.warning(f"Failed to extract topic from message: {exc}")
        topic = None

    if error:
        logger.error("Message failed delivery.", extra={"error": error, "topic": topic})
    else:
        logger.debug("Message delivered.", extra={"topic": topic})
