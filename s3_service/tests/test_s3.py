import io
from unittest.mock import MagicMock, Mock, patch

import botocore.exceptions
import pytest
from faker import Faker
from stream_chunker.s3_service.exceptions import RequiredBucketNotFoundException
from stream_chunker.s3_service.s3 import S3Service, S3ServiceConfig

fake = Faker()


@patch("boto3.client")
def test_init(boto3_client):
    config = S3ServiceConfig(
        s3_access_key=fake.password(),
        s3_endpoint_url=fake.url(),
        s3_secret_key=fake.password(),
    )
    S3Service(config)

    boto3_client.assert_called_once_with(
        "s3",
        endpoint_url=config.s3_endpoint_url,
        aws_access_key_id=config.s3_access_key,
        aws_secret_access_key=config.s3_secret_key,
    )


@pytest.fixture(name="s3_service")
@patch("boto3.client")
def fixture_s3_service(client: Mock):
    config = S3ServiceConfig(
        s3_access_key=fake.password(),
        s3_endpoint_url=fake.url(),
        s3_secret_key=fake.password(),
    )
    client.return_value(MagicMock())
    return S3Service(config)


def test_has_bucket_no_throw_true(s3_service):
    bucket_name = fake.name()
    result = s3_service.has_bucket(bucket=bucket_name)

    assert result is True


def test_has_bucket_no_throw_exits(s3_service):
    bucket_name = fake.name()
    result = s3_service.has_bucket(bucket=bucket_name, throw=False)

    assert result is True


def test_has_bucket_no_throw_not_exists(s3_service):
    bucket_name = fake.name()
    s3_service.client.head_bucket.side_effect = botocore.exceptions.ClientError(
        MagicMock(), MagicMock()
    )
    result = s3_service.has_bucket(bucket=bucket_name)

    assert result is False


def test_has_bucket_throw_not_exists(s3_service):
    bucket_name = fake.name()
    s3_service.client.head_bucket.side_effect = botocore.exceptions.ClientError(
        MagicMock(), MagicMock()
    )

    with pytest.raises(RequiredBucketNotFoundException):
        s3_service.has_bucket(bucket=bucket_name, throw=True)


def test_upload_buffer_exception(s3_service):
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())
    bucket_name = fake.name()
    key = fake.name()

    s3_service.client.upload_fileobj.side_effect = Exception()

    with pytest.raises(Exception):
        s3_service.upload_buffer(buffer=buffer_bytes, bucket=bucket_name, key=key)

    s3_service.client.upload_fileobj.assert_called_once_with(
        Fileobj=buffer_bytes,
        Bucket=bucket_name,
        Key=key,
    )


def test_upload_file_exception(s3_service):
    file_path = fake.file_path()
    bucket_name = fake.name()
    key = fake.name()

    s3_service.client.upload_file.side_effect = Exception()

    with pytest.raises(Exception):
        s3_service.upload_file(file_path=file_path, bucket=bucket_name, key=key)

    s3_service.client.upload_file.assert_called_once_with(
        Filename=file_path,
        Bucket=bucket_name,
        Key=key,
    )
