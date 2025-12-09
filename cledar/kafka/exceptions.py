class KafkaProducerNotConnectedError(Exception):
    """
    Custom exception for KafkaProducer to indicate it is not connected.
    """


class KafkaConsumerNotConnectedError(Exception):
    """
    Custom exception for KafkaConsumer to indicate it is not connected.
    """


class KafkaConnectionError(Exception):
    """
    Custom exception to indicate connection failures.
    """


class KafkaConsumerError(Exception):
    """
    Custom exception for KafkaConsumer to indicate errors.
    """
