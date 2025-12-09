CE_SOURCE_HEADER = "ce-source"


def get_input_topic(headers: dict[str, str]) -> str | None:
    """Extract the Kafka topic name from CloudEvents source header.

    Parses the 'ce-source' header value which is expected to be in the format
    'prefix#topic_name' and returns the topic name after the '#' delimiter.

    Args:
        headers: Dictionary of HTTP headers containing CloudEvents metadata.

    Returns:
        The extracted topic name if the header exists, contains '#', and has
        a non-empty topic name after the delimiter. Returns None otherwise.

    Example:
        >>> headers = {"ce-source": "kafka://cluster#my-topic"}
        >>> get_input_topic(headers)
        'my-topic'
    """
    source = headers.get(CE_SOURCE_HEADER)
    if not source or "#" not in source:
        return None

    topic = source.split("#", 1)[1].strip()
    return topic if topic else None
