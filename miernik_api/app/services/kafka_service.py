import json
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi.encoders import jsonable_encoder
from ..exceptions import bad_gateway_error
from ..config import settings

async def produce_batch(topic: str, values: list[bytes], key: str | None = None) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try: #TODO(pkomor): consider send_batch
        for value in values:
            await producer.send_and_wait(topic, key=key, value=value)
    finally:
        await producer.stop()

async def prepare_and_send_messages(topic: str, messages: list) -> None:
    encoded_messages = [json.dumps(jsonable_encoder(msg)).encode("utf-8") for msg in messages]
    try:
        #TODO(pkomor): set key
        # key = None -> random partition 
        await produce_batch(topic, encoded_messages, key=None)
    except KafkaError as error:
        raise bad_gateway_error(f"Failed to send data to Kafka: {str(error)}") from error
