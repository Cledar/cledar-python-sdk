def build_topic(topic_name: str, prefix: str | None) -> str:
    return prefix + topic_name if prefix else topic_name
