from cledar.kserve.utils import get_input_topic


def test_get_input_topic_valid_source() -> None:
    headers = {"ce-source": "kafka://cluster#my-topic"}
    result = get_input_topic(headers)

    assert result == "my-topic"


def test_get_input_topic_with_whitespace() -> None:
    headers = {"ce-source": "kafka://cluster# my-topic "}
    result = get_input_topic(headers)

    assert result == "my-topic"


def test_get_input_topic_missing_header() -> None:
    headers: dict[str, str] = {}
    result = get_input_topic(headers)

    assert result is None


def test_get_input_topic_no_delimiter() -> None:
    headers = {"ce-source": "kafka://cluster/my-topic"}
    result = get_input_topic(headers)

    assert result is None


def test_get_input_topic_empty_after_delimiter() -> None:
    headers = {"ce-source": "kafka://cluster#"}
    result = get_input_topic(headers)

    assert result is None


def test_get_input_topic_only_whitespace_after_delimiter() -> None:
    headers = {"ce-source": "kafka://cluster#   "}
    result = get_input_topic(headers)

    assert result is None


def test_get_input_topic_multiple_delimiters() -> None:
    headers = {"ce-source": "kafka://cluster#my-topic#with-hash"}
    result = get_input_topic(headers)

    assert result == "my-topic#with-hash"


def test_get_input_topic_empty_source_value() -> None:
    headers = {"ce-source": ""}
    result = get_input_topic(headers)

    assert result is None


def test_get_input_topic_complex_topic_name() -> None:
    headers = {"ce-source": "kafka://prod.cluster.example.com#namespace.my-topic-v2"}
    result = get_input_topic(headers)

    assert result == "namespace.my-topic-v2"
