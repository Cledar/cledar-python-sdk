class KafkaProducerNotConnectedError(Exception):
    """Custom exception for KafkaProducer to indicate it is not connected."""


class KafkaConnectionError(Exception):
    """Custom exception for KafkaProducer to indicate connection failures."""
