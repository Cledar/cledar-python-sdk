from cledar.kafka.utils.messages import extract_id_from_value


def test_extract_id_from_value_correct_value() -> None:
    input_value = '{"id": "3"}'
    expected_id = "3"
    extracted_id = extract_id_from_value(input_value)

    assert expected_id == extracted_id, "Extracted and expected id doesn't match"


def test_extract_id_from_value_incorrect_value() -> None:
    input_value = "s"
    expected_id = "<unknown_id>"
    extracted_id = extract_id_from_value(input_value)

    assert expected_id == extracted_id, "Extracted and expected id doesn't match"


def test_extract_id_from_value_none_value() -> None:
    input_value = None
    expected_id = "<unknown_id>"
    extracted_id = extract_id_from_value(input_value)

    assert expected_id == extracted_id, "Extracted and expected id doesn't match"
