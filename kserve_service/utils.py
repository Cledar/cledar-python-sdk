def get_input_topic(headers: dict[str, str]) -> str | None:
    source = headers.get("ce-source")
    if not source or "#" not in source:
        return None
    return source.split("#", 1)[1]
