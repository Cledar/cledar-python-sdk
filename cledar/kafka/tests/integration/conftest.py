from collections.abc import Generator

import pytest
from testcontainers.kafka import KafkaContainer

from cledar.kafka.clients.consumer import KafkaConsumer
from cledar.kafka.clients.producer import KafkaProducer
from cledar.kafka.config.schemas import KafkaConsumerConfig, KafkaProducerConfig


@pytest.fixture(scope="session")
def kafka_container() -> Generator[KafkaContainer, None, None]:
    kafka = KafkaContainer("confluentinc/cp-kafka:7.4.0")
    kafka = kafka.with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
    kafka = kafka.with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
    kafka = kafka.with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
    kafka = kafka.with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    kafka = kafka.with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
    kafka = kafka.with_env("KAFKA_LOG_RETENTION_HOURS", "1")
    with kafka as container:
        yield container


@pytest.fixture
def kafka_bootstrap_servers(kafka_container: KafkaContainer) -> str:
    # testcontainers returns a str, but mypy sees Any without stubs
    server: str = str(kafka_container.get_bootstrap_server())
    return server


@pytest.fixture
def producer_config(kafka_bootstrap_servers: str) -> KafkaProducerConfig:
    return KafkaProducerConfig(
        kafka_servers=kafka_bootstrap_servers,
        kafka_group_id="integration-test-producer",
        kafka_topic_prefix="integration-test.",
        kafka_block_buffer_time_sec=1,
        kafka_connection_check_timeout_sec=5,
        kafka_connection_check_interval_sec=10,
    )


@pytest.fixture
def consumer_config(kafka_bootstrap_servers: str) -> KafkaConsumerConfig:
    return KafkaConsumerConfig(
        kafka_servers=kafka_bootstrap_servers,
        kafka_group_id="integration-test-consumer",
        kafka_offset="earliest",
        kafka_topic_prefix="integration-test.",
        kafka_block_consumer_time_sec=1,
        kafka_connection_check_timeout_sec=5,
        kafka_auto_commit_interval_ms=1000,
        kafka_connection_check_interval_sec=10,
    )


@pytest.fixture
def producer(
    producer_config: KafkaProducerConfig,
) -> Generator[KafkaProducer, None, None]:
    p = KafkaProducer(producer_config)
    p.connect()
    try:
        yield p
    finally:
        p.shutdown()


@pytest.fixture
def consumer(
    consumer_config: KafkaConsumerConfig,
) -> Generator[KafkaConsumer, None, None]:
    c = KafkaConsumer(consumer_config)
    c.connect()
    try:
        yield c
    finally:
        c.shutdown()
