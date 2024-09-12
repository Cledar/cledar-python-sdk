import pytest
from s3_service.url import IncorrectSchemaException, S3ItemMetadata, parse_url


def test_ok():
    metadata = parse_url("s3://bucket/path/to/item.extension")

    assert metadata == S3ItemMetadata(
        bucket="bucket",
        key="path/to/item.extension",
    )


def test_incorrect_schema():
    with pytest.raises(IncorrectSchemaException):
        parse_url("https://any.domain")
