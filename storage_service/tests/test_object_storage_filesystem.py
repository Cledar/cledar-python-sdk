# mypy: disable-error-code=method-assign
# pylint: disable=import-error
import io
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker
from storage_service.object_storage import ObjectStorageService, ObjectStorageServiceConfig  # type: ignore[import-not-found]

fake = Faker()


@pytest.fixture(name="object_storage_service_filesystem")
@patch("fsspec.filesystem")
def fixture_object_storage_service_filesystem(
    fsspec_client: MagicMock, object_storage_config: ObjectStorageServiceConfig
) -> ObjectStorageService:
    fsspec_client.return_value = MagicMock()
    return ObjectStorageService(object_storage_config)


def test_upload_buffer_filesystem_success(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test successful buffer upload to filesystem."""
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())
    destination_path = fake.file_path()

    mock_file = MagicMock()
    mock_file.write = MagicMock()

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    object_storage_service_filesystem.local_client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    object_storage_service_filesystem.upload_buffer(
        buffer=buffer_bytes, destination_path=destination_path
    )

    object_storage_service_filesystem.local_client.open.assert_called_once_with(
        path=destination_path, mode="wb"
    )
    mock_file.write.assert_called_once_with(buffer_bytes.getbuffer())


def test_upload_buffer_filesystem_exception(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test buffer upload to filesystem with write exception."""
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())
    destination_path = fake.file_path()

    class Writer:
        def write(self, _b: bytes) -> None:
            raise RuntimeError("write failed")

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield Writer()

    object_storage_service_filesystem.local_client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    with pytest.raises(RuntimeError):
        object_storage_service_filesystem.upload_buffer(
            buffer=buffer_bytes, destination_path=destination_path
        )
    object_storage_service_filesystem.local_client.open.assert_called_once()


def test_upload_buffer_filesystem_missing_params(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test buffer upload with missing parameters."""
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())

    with pytest.raises(
        ValueError, match="Either destination_path or bucket and key must be provided"
    ):
        object_storage_service_filesystem.upload_buffer(buffer=buffer_bytes)


def test_read_file_filesystem_success(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test successful file read from filesystem."""
    path = fake.file_path()
    expected_content = fake.text().encode()

    mock_file = MagicMock()
    mock_file.read.return_value = expected_content

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    object_storage_service_filesystem.local_client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    result = object_storage_service_filesystem.read_file(path=path)
    assert result == expected_content
    object_storage_service_filesystem.local_client.open.assert_called_once_with(
        path=path, mode="rb"
    )


def test_read_file_filesystem_retry_mechanism(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test file read retry mechanism on filesystem errors."""
    path = fake.file_path()
    expected_content = fake.text().encode()

    mock_file = MagicMock()
    mock_file.read.return_value = expected_content

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    call_count = 0

    def side_effect(*_args, **_kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise OSError("Temporary error")
        return open_cm()

    object_storage_service_filesystem.local_client.open = MagicMock(
        side_effect=side_effect
    )

    result = object_storage_service_filesystem.read_file(path=path, max_tries=3)

    assert result == expected_content
    assert call_count == 3


def test_read_file_filesystem_max_retries_exceeded(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test file read when max retries are exceeded."""
    path = fake.file_path()

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        raise OSError("Persistent error")

    object_storage_service_filesystem.local_client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    with pytest.raises(OSError):
        object_storage_service_filesystem.read_file(path=path, max_tries=2)


def test_read_file_filesystem_missing_params(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test file read with missing parameters."""
    with pytest.raises(
        ValueError, match="Either path or bucket and key must be provided"
    ):
        object_storage_service_filesystem.read_file()


def test_upload_file_filesystem_success(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test successful file upload to filesystem."""
    file_path = fake.file_path()
    destination_path = fake.file_path()

    object_storage_service_filesystem.client.put = MagicMock()

    object_storage_service_filesystem.upload_file(
        file_path=file_path, destination_path=destination_path
    )

    object_storage_service_filesystem.client.put.assert_called_once_with(
        lpath=file_path, rpath=destination_path
    )


def test_upload_file_filesystem_exception(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test file upload to filesystem with exception."""
    file_path = fake.file_path()
    destination_path = fake.file_path()

    object_storage_service_filesystem.client.put.side_effect = Exception(
        "Failed to upload file"
    )

    with pytest.raises(Exception, match="Failed to upload file"):
        object_storage_service_filesystem.upload_file(
            file_path=file_path, destination_path=destination_path
        )

    object_storage_service_filesystem.client.put.assert_called_once_with(
        lpath=file_path, rpath=destination_path
    )


def test_upload_file_filesystem_missing_params(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test file upload with missing parameters."""
    file_path = fake.file_path()

    with pytest.raises(
        ValueError, match="Either destination_path or bucket and key must be provided"
    ):
        object_storage_service_filesystem.upload_file(file_path=file_path)


def test_upload_file_filesystem_with_bucket_key_should_use_s3(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test that upload_file with bucket and key uses S3 client, not filesystem."""
    file_path = fake.file_path()
    bucket_name = fake.name()
    key = fake.name()

    object_storage_service_filesystem.client.put = MagicMock()

    object_storage_service_filesystem.upload_file(
        file_path=file_path, bucket=bucket_name, key=key
    )

    object_storage_service_filesystem.client.put.assert_called_once_with(
        lpath=file_path, rpath=f"s3://{bucket_name}/{key}"
    )


def test_read_file_filesystem_with_bucket_key_should_use_s3(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test that read_file with bucket and key uses S3 client, not filesystem."""
    bucket_name = fake.name()
    key = fake.name()
    expected_content = fake.text().encode()

    mock_file = MagicMock()
    mock_file.read.return_value = expected_content

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    object_storage_service_filesystem.client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    result = object_storage_service_filesystem.read_file(bucket=bucket_name, key=key)

    assert result == expected_content
    object_storage_service_filesystem.client.open.assert_called_once_with(
        path=f"s3://{bucket_name}/{key}", mode="rb"
    )


def test_upload_buffer_filesystem_with_bucket_key_should_use_s3(
    object_storage_service_filesystem: ObjectStorageService,
) -> None:
    """Test that upload_buffer with bucket and key uses S3 client, not filesystem."""
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())
    bucket_name = fake.name()
    key = fake.name()

    mock_file = MagicMock()
    mock_file.write = MagicMock()

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    object_storage_service_filesystem.client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    object_storage_service_filesystem.upload_buffer(
        buffer=buffer_bytes, bucket=bucket_name, key=key
    )

    object_storage_service_filesystem.client.open.assert_called_once_with(
        path=f"s3://{bucket_name}/{key}", mode="wb"
    )
    mock_file.write.assert_called_once_with(buffer_bytes.getbuffer())
