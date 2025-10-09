# mypy: disable-error-code=method-assign
# pylint: disable=import-error
import io
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker
from storage_service.exceptions import RequiredBucketNotFoundException  # type: ignore[import-not-found]
from storage_service.object_storage import ObjectStorageService, ObjectStorageServiceConfig  # type: ignore[import-not-found]

fake = Faker()


@patch("fsspec.filesystem")
def test_init(
    fsspec_client: MagicMock, object_storage_config: ObjectStorageServiceConfig
) -> None:
    ObjectStorageService(object_storage_config)

    fsspec_client.assert_any_call(
        "s3",
        key=object_storage_config.s3_access_key,
        secret=object_storage_config.s3_secret_key,
        client_kwargs={"endpoint_url": object_storage_config.s3_endpoint_url},
    )
    fsspec_client.assert_any_call(
        "file",
    )


@pytest.fixture(name="object_storage_service")
@patch("fsspec.filesystem")
def fixture_object_storage_service(
    fsspec_client: MagicMock, object_storage_config: ObjectStorageServiceConfig
) -> ObjectStorageService:
    fsspec_client.return_value = MagicMock()
    return ObjectStorageService(object_storage_config)


def test_has_bucket_no_throw_true(object_storage_service: ObjectStorageService) -> None:
    bucket_name = fake.name()
    result = object_storage_service.has_bucket(bucket=bucket_name)

    assert result is True


def test_has_bucket_no_throw_exists(
    object_storage_service: ObjectStorageService,
) -> None:
    bucket_name = fake.name()
    result = object_storage_service.has_bucket(bucket=bucket_name, throw=False)

    assert result is True


def test_has_bucket_no_throw_not_exists(
    object_storage_service: ObjectStorageService,
) -> None:
    bucket_name = fake.name()
    object_storage_service.client.ls.side_effect = Exception()
    result = object_storage_service.has_bucket(bucket=bucket_name)

    assert result is False


def test_has_bucket_throw_not_exists(
    object_storage_service: ObjectStorageService,
) -> None:
    bucket_name = fake.name()
    object_storage_service.client.ls.side_effect = Exception()

    with pytest.raises(RequiredBucketNotFoundException):
        object_storage_service.has_bucket(bucket=bucket_name, throw=True)


def test_upload_buffer_exception(object_storage_service: ObjectStorageService) -> None:
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

    object_storage_service.client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    with pytest.raises(RuntimeError):
        object_storage_service.upload_buffer(
            buffer=buffer_bytes, bucket=bucket_name, key=key
        )
    object_storage_service.client.open.assert_called_once()


def test_upload_file_exception(object_storage_service: ObjectStorageService) -> None:
    file_path = fake.file_path()
    bucket_name = fake.name()
    key = fake.name()

    object_storage_service.client.put.side_effect = Exception()

    with pytest.raises(Exception):
        object_storage_service.upload_file(
            file_path=file_path, bucket=bucket_name, key=key
        )

    object_storage_service.client.put.assert_called_once_with(
        lpath=file_path, rpath=f"s3://{bucket_name}/{key}"
    )


def test_read_file_exception(object_storage_service: ObjectStorageService) -> None:
    bucket_name = fake.name()
    key = fake.name()

    # make fs.open raise
    object_storage_service.client.open.side_effect = Exception()

    with pytest.raises(Exception):
        object_storage_service.read_file(bucket=bucket_name, key=key)
    object_storage_service.client.open.assert_called_once()


def test_verify_and_upload_test_file(
    object_storage_service: ObjectStorageService,
) -> None:
    bucket = fake.name()
    object_storage_service.client.ls.return_value = [fake.name()]
    object_storage_service.has_bucket = MagicMock()
    object_storage_service.upload_buffer = MagicMock()
    object_storage_service.read_file = MagicMock()

    with patch("io.BytesIO", new=MagicMock()) as MockBytesIO:
        mock_buffer = MockBytesIO.return_value
        object_storage_service.verify_and_upload_test_file(bucket, throw=True)
        object_storage_service.client.ls.assert_called_once()
        object_storage_service.has_bucket.assert_called_once_with(bucket, throw=True)
        object_storage_service.upload_buffer.assert_called_once()
        object_storage_service.read_file.assert_called_once()
        mock_buffer.write.assert_called()
        mock_buffer.seek.assert_called_with(0)
        mock_buffer.close.assert_called()
