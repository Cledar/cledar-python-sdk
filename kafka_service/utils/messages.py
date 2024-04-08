import json

from ..logger import logger

_UNKNOWN_ID_PLACEHOLDER = "<unknown_id>"
_ID_FIELD_KEY = "id"


def extract_id_from_value(value: str | None) -> str:
    msg_id: str = _UNKNOWN_ID_PLACEHOLDER
    if value is None:
        return msg_id

    try:
        parsed_value = json.loads(value).get(_ID_FIELD_KEY, _UNKNOWN_ID_PLACEHOLDER)
    except (json.JSONDecodeError, TypeError, AttributeError) as e:
        logger.error(f"Decoding for id failed. {e}")
        return _UNKNOWN_ID_PLACEHOLDER

    msg_id = (
        str(parsed_value)
        if parsed_value != _UNKNOWN_ID_PLACEHOLDER
        else _UNKNOWN_ID_PLACEHOLDER
    )
    return msg_id


consumer_not_connected_msg = "KafkaConsumer is not connected. Call connect first."
