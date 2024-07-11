# pylint: disable=unused-argument, protected-access
from unittest.mock import Mock, patch, MagicMock

import pytest

from stream_chunker.s3_service.s3 import S3Service, S3ServiceConfig
from faker import Faker
import botocore.exceptions
from stream_chunker.s3_service.exceptions import RequiredBucketNotFoundException

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


config = S3ServiceConfig(
    s3_access_key=fake.password(),
    s3_endpoint_url=fake.url(),
    s3_secret_key=fake.password(),
)


@pytest.fixture(name="s3_service")
@patch("boto3.client")
def fixture_s3_service(client: Mock):
    client.return_value(MagicMock())
    return S3Service(config)


def test_has_bucket_no_throw_true(s3_service):
    bucket_name = fake.name()
    result = s3_service.has_bucket(bucket=bucket_name)

    assert result == True


def test_has_bucket_no_throw_exits(s3_service):
    bucket_name = fake.name()
    result = s3_service.has_bucket(bucket=bucket_name, throw=False)

    assert result == True


def test_has_bucket_no_throw_not_exists(s3_service):
    bucket_name = fake.name()
    s3_service.client.head_bucket.side_effect = botocore.exceptions.ClientError(
        MagicMock(), MagicMock()
    )
    result = s3_service.has_bucket(bucket=bucket_name)

    assert result == False


def test_has_bucket_throw_not_exists(s3_service):
    bucket_name = fake.name()
    s3_service.client.head_bucket.side_effect = botocore.exceptions.ClientError(
        MagicMock(), MagicMock()
    )
    
    with pytest.raises(RequiredBucketNotFoundException):
        s3_service.has_bucket(bucket=bucket_name, throw=True)
