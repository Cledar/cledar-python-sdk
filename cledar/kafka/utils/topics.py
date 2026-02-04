"""Kafka topic utilities."""


def build_topic(topic_name: str, prefix: str | None) -> str:
    """Build a topic name by optionally prepending a prefix.

    Args:
        topic_name: The base topic name.
        prefix: An optional prefix to prepend.

    Returns:
        str: The full topic name.

    """
    return prefix + topic_name if prefix else topic_name
