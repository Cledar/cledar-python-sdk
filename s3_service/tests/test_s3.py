# mypy: disable-error-code=method-assign
import io
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest
from faker import Faker
from s3_service.exceptions import RequiredBucketNotFoundException
from s3_service.s3 import S3Service, S3ServiceConfig

fake = Faker()


@patch("boto3.client")
def test_init(boto3_client: MagicMock, s3_config: S3ServiceConfig) -> None:
    S3Service(s3_config)

    boto3_client.assert_called_once_with(
        "s3",
        endpoint_url=s3_config.s3_endpoint_url,
        aws_access_key_id=s3_config.s3_access_key,
        aws_secret_access_key=s3_config.s3_secret_key,
    )


@pytest.fixture(name="s3_service")
@patch("boto3.client")
def fixture_s3_service(client: MagicMock, s3_config: S3ServiceConfig) -> S3Service:
    client.return_value(MagicMock())
    return S3Service(s3_config)


def test_has_bucket_no_throw_true(s3_service: S3Service) -> None:
    bucket_name = fake.name()
    result = s3_service.has_bucket(bucket=bucket_name)

    assert result is True


def test_has_bucket_no_throw_exists(s3_service: S3Service) -> None:
    bucket_name = fake.name()
    result = s3_service.has_bucket(bucket=bucket_name, throw=False)

    assert result is True


def test_has_bucket_no_throw_not_exists(s3_service: S3Service) -> None:
    bucket_name = fake.name()
    s3_service.client.head_bucket.side_effect = botocore.exceptions.ClientError(
        MagicMock(), MagicMock()
    )
    result = s3_service.has_bucket(bucket=bucket_name)

    assert result is False


def test_has_bucket_throw_not_exists(s3_service: S3Service) -> None:
    bucket_name = fake.name()
    s3_service.client.head_bucket.side_effect = botocore.exceptions.ClientError(
        MagicMock(), MagicMock()
    )

    with pytest.raises(RequiredBucketNotFoundException):
        s3_service.has_bucket(bucket=bucket_name, throw=True)


def test_upload_buffer_exception(s3_service: S3Service) -> None:
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


def test_upload_file_exception(s3_service: S3Service) -> None:
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


def test_read_file_exception(s3_service: S3Service) -> None:
    bucket_name = fake.name()
    key = fake.name()

    s3_service.client.get_object.side_effect = Exception()

    with pytest.raises(Exception):
        s3_service.read_file(bucket=bucket_name, key=key)

    s3_service.client.get_object.assert_called_once_with(
        Bucket=bucket_name,
        Key=key,
    )


def test_verify_and_upload_test_file(s3_service: S3Service) -> None:
    bucket = fake.name()
    test_bucket = fake.name()
    s3_service.client.list_buckets.return_value = {"Buckets": [{"Name": test_bucket}]}
    s3_service.has_bucket = MagicMock()
    s3_service.upload_buffer = MagicMock()
    s3_service.read_file = MagicMock()

    with patch("io.BytesIO", new=MagicMock()) as MockBytesIO:
        mock_buffer = MockBytesIO.return_value
        s3_service.verify_and_upload_test_file(bucket, throw=True)
        s3_service.client.list_buckets.assert_called_once()
        s3_service.has_bucket.assert_called_once_with(bucket, throw=True)
        s3_service.upload_buffer.assert_called_once()
        s3_service.read_file.assert_called_once()
        mock_buffer.write.assert_called()
        mock_buffer.seek.assert_called_with(0)
        mock_buffer.close.assert_called()
