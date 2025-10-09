# mypy: disable-error-code=method-assign
# pylint: disable=import-error
import io
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker
from s3_service.exceptions import RequiredBucketNotFoundException  # type: ignore[import-not-found]
from s3_service.s3 import S3Service, S3ServiceConfig  # type: ignore[import-not-found]

fake = Faker()


@patch("fsspec.filesystem")
def test_init(fs_ctor: MagicMock, s3_config: S3ServiceConfig) -> None:
    S3Service(s3_config)

    fs_ctor.assert_called_once_with(
        "s3",
        key=s3_config.s3_access_key,
        secret=s3_config.s3_secret_key,
        client_kwargs={"endpoint_url": s3_config.s3_endpoint_url},
    )


@pytest.fixture(name="s3_service")
@patch("fsspec.filesystem")
def fixture_s3_service(fs_ctor: MagicMock, s3_config: S3ServiceConfig) -> S3Service:
    fs_ctor.return_value = MagicMock()
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
    s3_service.client.ls.side_effect = Exception()
    result = s3_service.has_bucket(bucket=bucket_name)

    assert result is False


def test_has_bucket_throw_not_exists(s3_service: S3Service) -> None:
    bucket_name = fake.name()
    s3_service.client.ls.side_effect = Exception()

    with pytest.raises(RequiredBucketNotFoundException):
        s3_service.has_bucket(bucket=bucket_name, throw=True)


def test_upload_buffer_exception(s3_service: S3Service) -> None:
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())
    bucket_name = fake.name()
    key = fake.name()

    # mock fs.open as context manager that raises on write
    class Writer:
        def write(self, _b: bytes) -> None:
            raise RuntimeError("write failed")

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield Writer()

    s3_service.client.open = MagicMock(side_effect=lambda *a, **k: open_cm())

    with pytest.raises(RuntimeError):
        s3_service.upload_buffer(buffer=buffer_bytes, bucket=bucket_name, key=key)
    s3_service.client.open.assert_called_once()


def test_upload_file_exception(s3_service: S3Service) -> None:
    file_path = fake.file_path()
    bucket_name = fake.name()
    key = fake.name()

    s3_service.client.put.side_effect = Exception()

    with pytest.raises(Exception):
        s3_service.upload_file(file_path=file_path, bucket=bucket_name, key=key)

    s3_service.client.put.assert_called_once_with(file_path, f"{bucket_name}/{key}")


def test_read_file_exception(s3_service: S3Service) -> None:
    bucket_name = fake.name()
    key = fake.name()

    # make fs.open raise
    s3_service.client.open.side_effect = Exception()

    with pytest.raises(Exception):
        s3_service.read_file(bucket=bucket_name, key=key)
    s3_service.client.open.assert_called_once()


def test_verify_and_upload_test_file(s3_service: S3Service) -> None:
    bucket = fake.name()
    s3_service.client.ls.return_value = [fake.name()]
    s3_service.has_bucket = MagicMock()
    s3_service.upload_buffer = MagicMock()
    s3_service.read_file = MagicMock()

    with patch("io.BytesIO", new=MagicMock()) as MockBytesIO:
        mock_buffer = MockBytesIO.return_value
        s3_service.verify_and_upload_test_file(bucket, throw=True)
        s3_service.client.ls.assert_called_once()
        s3_service.has_bucket.assert_called_once_with(bucket, throw=True)
        s3_service.upload_buffer.assert_called_once()
        s3_service.read_file.assert_called_once()
        mock_buffer.write.assert_called()
        mock_buffer.seek.assert_called_with(0)
        mock_buffer.close.assert_called()
