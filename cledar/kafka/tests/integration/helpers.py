import time

from pydantic import BaseModel

from cledar.kafka.clients.consumer import KafkaConsumer
from cledar.kafka.clients.producer import KafkaProducer
from cledar.kafka.models.message import KafkaMessage


class E2EData(BaseModel):
    id: str
    message: str
    timestamp: float

    def to_json(self) -> str:
        return self.model_dump_json()


def ensure_topic_and_subscribe(
    producer: KafkaProducer,
    consumer: KafkaConsumer,
    topic: str,
    init_payload: str = '{"id":"init","message":"init"}',
    create_wait: float = 2.0,
    subscribe_wait: float = 1.0,
) -> None:
    producer.send(topic=topic, value=init_payload, key="init-key")
    time.sleep(create_wait)
    consumer.subscribe([topic])
    time.sleep(subscribe_wait)


def consume_until(
    consumer: KafkaConsumer,
    expected_count: int,
    timeout_seconds: float = 10.0,
    idle_sleep: float = 0.2,
) -> list[KafkaMessage]:
    deadline = time.time() + timeout_seconds
    received: list[KafkaMessage] = []
    while len(received) < expected_count and time.time() < deadline:
        msg = consumer.consume_next()
        if msg is not None:
            received.append(msg)
        else:
            time.sleep(idle_sleep)
    return received
