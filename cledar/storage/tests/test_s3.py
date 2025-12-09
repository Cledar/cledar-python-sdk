# mypy: disable-error-code=no-untyped-def
import io
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker

from cledar.storage.exceptions import (
    CheckFileExistenceError,
    CopyFileError,
    DeleteFileError,
    DownloadFileError,
    GetFileInfoError,
    GetFileSizeError,
    ListObjectsError,
    MoveFileError,
    ReadFileError,
    RequiredBucketNotFoundError,
    UploadBufferError,
    UploadFileError,
)
from cledar.storage.models import ObjectStorageServiceConfig
from cledar.storage.object_storage import ObjectStorageService

fake = Faker()


@pytest.fixture(name="object_storage_service")
@patch("fsspec.filesystem")
def fixture_object_storage_service(
    fsspec_client: MagicMock, object_storage_config: ObjectStorageServiceConfig
) -> ObjectStorageService:
    fsspec_client.return_value = MagicMock()
    return ObjectStorageService(object_storage_config)


def test_upload_file_filesystem_with_bucket_key_should_use_s3(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test that upload_file with bucket and key uses S3 client, not filesystem.
    """
    file_path = fake.file_path()
    bucket_name = fake.name()
    key = fake.name()

    object_storage_service.client.put = MagicMock()

    object_storage_service.upload_file(file_path=file_path, bucket=bucket_name, key=key)

    object_storage_service.client.put.assert_called_once_with(
        lpath=file_path, rpath=f"s3://{bucket_name}/{key}"
    )


def test_read_file_filesystem_with_bucket_key_should_use_s3(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test that read_file with bucket and key uses S3 client, not filesystem.
    """
    bucket_name = fake.name()
    key = fake.name()
    expected_content = fake.text().encode()

    mock_file = MagicMock()
    mock_file.read.return_value = expected_content

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    object_storage_service.client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    result = object_storage_service.read_file(bucket=bucket_name, key=key)

    assert result == expected_content
    object_storage_service.client.open.assert_called_once_with(
        path=f"s3://{bucket_name}/{key}", mode="rb"
    )


def test_upload_buffer_filesystem_with_bucket_key_should_use_s3(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test that upload_buffer with bucket and key uses S3 client, not filesystem.
    """
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())
    bucket_name = fake.name()
    key = fake.name()

    mock_file = MagicMock()
    mock_file.write = MagicMock()

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    object_storage_service.client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    object_storage_service.upload_buffer(
        buffer=buffer_bytes, bucket=bucket_name, key=key
    )

    object_storage_service.client.open.assert_called_once_with(
        path=f"s3://{bucket_name}/{key}", mode="wb"
    )
    mock_file.write.assert_called_once_with(buffer_bytes.getbuffer())


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
    object_storage_service.client.ls.side_effect = OSError("Connection failed")
    result = object_storage_service.has_bucket(bucket=bucket_name)

    assert result is False


def test_has_bucket_throw_not_exists(
    object_storage_service: ObjectStorageService,
) -> None:
    bucket_name = fake.name()
    object_storage_service.client.ls.side_effect = OSError("Connection failed")

    with pytest.raises(RequiredBucketNotFoundError):
        object_storage_service.has_bucket(bucket=bucket_name, throw=True)


def test_upload_buffer_exception(object_storage_service: ObjectStorageService) -> None:
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())
    bucket_name = fake.name()
    key = fake.name()

    class Writer:
        def write(self, _b: bytes) -> None:
            raise UploadBufferError

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield Writer()

    object_storage_service.client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    with pytest.raises(UploadBufferError):
        object_storage_service.upload_buffer(
            buffer=buffer_bytes, bucket=bucket_name, key=key
        )
    object_storage_service.client.open.assert_called_once()


def test_upload_file_exception(object_storage_service: ObjectStorageService) -> None:
    file_path = fake.file_path()
    bucket_name = fake.name()
    key = fake.name()

    object_storage_service.client.put.side_effect = OSError("Network error")

    with pytest.raises(UploadFileError):
        object_storage_service.upload_file(
            file_path=file_path, bucket=bucket_name, key=key
        )

    object_storage_service.client.put.assert_called_once_with(
        lpath=file_path, rpath=f"s3://{bucket_name}/{key}"
    )


def test_read_file_exception(object_storage_service: ObjectStorageService) -> None:
    bucket_name = fake.name()
    key = fake.name()

    object_storage_service.client.open.side_effect = ReadFileError

    with pytest.raises(ReadFileError):
        object_storage_service.read_file(bucket=bucket_name, key=key)
    object_storage_service.client.open.assert_called_once()


def test_list_objects_recursive_s3(
    object_storage_service: ObjectStorageService,
) -> None:
    bucket = fake.name()
    prefix = "test/prefix"
    mock_objects = [
        f"s3://{bucket}/{prefix}/file1.txt",
        f"s3://{bucket}/{prefix}/file2.txt",
        f"s3://{bucket}/{prefix}/subfolder/file3.txt",
    ]
    object_storage_service.client.find.return_value = mock_objects

    result = object_storage_service.list_objects(
        bucket=bucket, prefix=prefix, recursive=True
    )

    assert len(result) == 3
    assert f"{prefix}/file1.txt" in result
    assert f"{prefix}/file2.txt" in result
    assert f"{prefix}/subfolder/file3.txt" in result
    object_storage_service.client.find.assert_called_once()


def test_list_objects_non_recursive_s3(
    object_storage_service: ObjectStorageService,
) -> None:
    bucket = fake.name()
    prefix = "test/prefix"
    mock_objects = [
        f"s3://{bucket}/{prefix}/file1.txt",
        f"s3://{bucket}/{prefix}/file2.txt",
    ]
    object_storage_service.client.ls.return_value = mock_objects

    result = object_storage_service.list_objects(
        bucket=bucket, prefix=prefix, recursive=False
    )

    assert len(result) == 2
    assert f"{prefix}/file1.txt" in result
    assert f"{prefix}/file2.txt" in result
    object_storage_service.client.ls.assert_called_once()


def test_list_objects_exception(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    object_storage_service.client.find.side_effect = OSError("List failed")

    with pytest.raises(ListObjectsError):
        object_storage_service.list_objects(bucket=bucket)


def test_delete_file_s3(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    key = "test/file.txt"

    object_storage_service.delete_file(bucket=bucket, key=key)

    object_storage_service.client.rm.assert_called_once_with(f"s3://{bucket}/{key}")


def test_delete_file_exception(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    object_storage_service.client.rm.side_effect = OSError("Delete failed")

    with pytest.raises(DeleteFileError):
        object_storage_service.delete_file(bucket=bucket, key=key)


def test_file_exists_true_s3(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    object_storage_service.client.exists.return_value = True

    result = object_storage_service.file_exists(bucket=bucket, key=key)

    assert result is True
    object_storage_service.client.exists.assert_called_once_with(f"s3://{bucket}/{key}")


def test_file_exists_false_s3(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    object_storage_service.client.exists.return_value = False

    result = object_storage_service.file_exists(bucket=bucket, key=key)

    assert result is False
    object_storage_service.client.exists.assert_called_once_with(f"s3://{bucket}/{key}")


def test_file_exists_exception(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    object_storage_service.client.exists.side_effect = OSError("Check failed")

    with pytest.raises(CheckFileExistenceError):
        object_storage_service.file_exists(bucket=bucket, key=key)


def test_download_file_s3(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    dest_path = "/tmp/downloaded_file.txt"

    object_storage_service.download_file(dest_path, bucket=bucket, key=key)

    object_storage_service.client.get.assert_called_once_with(
        f"s3://{bucket}/{key}", dest_path
    )


def test_download_file_retry_success(
    object_storage_service: ObjectStorageService,
) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    dest_path = "/tmp/downloaded_file.txt"

    object_storage_service.client.get.side_effect = [
        OSError("Network error"),
        OSError("Network error"),
        None,
    ]

    object_storage_service.download_file(dest_path, bucket=bucket, key=key, max_tries=3)

    assert object_storage_service.client.get.call_count == 3


def test_download_file_exception_after_retries(
    object_storage_service: ObjectStorageService,
) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    dest_path = "/tmp/downloaded_file.txt"
    object_storage_service.client.get.side_effect = OSError("Network error")

    with pytest.raises(DownloadFileError):
        object_storage_service.download_file(
            dest_path, bucket=bucket, key=key, max_tries=3
        )

    assert object_storage_service.client.get.call_count == 3


def test_get_file_size_s3(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    expected_size = 1024
    object_storage_service.client.info.return_value = {"size": expected_size}

    result = object_storage_service.get_file_size(bucket=bucket, key=key)

    assert result == expected_size
    object_storage_service.client.info.assert_called_once_with(f"s3://{bucket}/{key}")


def test_get_file_size_exception(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    object_storage_service.client.info.side_effect = OSError("Info failed")

    with pytest.raises(GetFileSizeError):
        object_storage_service.get_file_size(bucket=bucket, key=key)


def test_get_file_info_s3(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    expected_info = {
        "size": 1024,
        "LastModified": "2025-01-01T00:00:00Z",
        "ContentType": "text/plain",
    }
    object_storage_service.client.info.return_value = expected_info

    result = object_storage_service.get_file_info(bucket=bucket, key=key)

    assert result == expected_info
    object_storage_service.client.info.assert_called_once_with(f"s3://{bucket}/{key}")


def test_get_file_info_exception(object_storage_service: ObjectStorageService) -> None:
    bucket = fake.name()
    key = "test/file.txt"
    object_storage_service.client.info.side_effect = OSError("Info failed")

    with pytest.raises(GetFileInfoError):
        object_storage_service.get_file_info(bucket=bucket, key=key)


def test_copy_file_s3_to_s3(object_storage_service: ObjectStorageService) -> None:
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_bucket = fake.name()
    dest_key = "test/destination.txt"

    object_storage_service.copy_file(
        source_bucket=source_bucket,
        source_key=source_key,
        dest_bucket=dest_bucket,
        dest_key=dest_key,
    )

    object_storage_service.client.copy.assert_called_once_with(
        f"s3://{source_bucket}/{source_key}", f"s3://{dest_bucket}/{dest_key}"
    )


def test_copy_file_s3_to_local(object_storage_service: ObjectStorageService) -> None:
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_path = "/tmp/dest/file.txt"

    object_storage_service.copy_file(
        source_bucket=source_bucket, source_key=source_key, dest_path=dest_path
    )

    object_storage_service.client.copy.assert_called_once_with(
        f"s3://{source_bucket}/{source_key}", dest_path
    )


def test_copy_file_local_to_s3(object_storage_service: ObjectStorageService) -> None:
    source_path = "/tmp/source/file.txt"
    dest_bucket = fake.name()
    dest_key = "test/destination.txt"

    object_storage_service.copy_file(
        source_path=source_path, dest_bucket=dest_bucket, dest_key=dest_key
    )

    object_storage_service.client.copy.assert_called_once_with(
        source_path, f"s3://{dest_bucket}/{dest_key}"
    )


def test_copy_file_exception(object_storage_service: ObjectStorageService) -> None:
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_bucket = fake.name()
    dest_key = "test/destination.txt"
    object_storage_service.client.copy.side_effect = OSError("Copy failed")

    with pytest.raises(CopyFileError):
        object_storage_service.copy_file(
            source_bucket=source_bucket,
            source_key=source_key,
            dest_bucket=dest_bucket,
            dest_key=dest_key,
        )


def test_move_file_s3_to_s3(object_storage_service: ObjectStorageService) -> None:
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_bucket = fake.name()
    dest_key = "test/destination.txt"

    object_storage_service.move_file(
        source_bucket=source_bucket,
        source_key=source_key,
        dest_bucket=dest_bucket,
        dest_key=dest_key,
    )

    object_storage_service.client.move.assert_called_once_with(
        f"s3://{source_bucket}/{source_key}", f"s3://{dest_bucket}/{dest_key}"
    )


def test_move_file_s3_to_local(object_storage_service: ObjectStorageService) -> None:
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_path = "/tmp/dest/file.txt"

    object_storage_service.move_file(
        source_bucket=source_bucket, source_key=source_key, dest_path=dest_path
    )

    object_storage_service.client.move.assert_called_once_with(
        f"s3://{source_bucket}/{source_key}", dest_path
    )


def test_move_file_local_to_s3(object_storage_service: ObjectStorageService) -> None:
    source_path = "/tmp/source/file.txt"
    dest_bucket = fake.name()
    dest_key = "test/destination.txt"

    object_storage_service.move_file(
        source_path=source_path, dest_bucket=dest_bucket, dest_key=dest_key
    )

    object_storage_service.client.move.assert_called_once_with(
        source_path, f"s3://{dest_bucket}/{dest_key}"
    )


def test_move_file_exception(object_storage_service: ObjectStorageService) -> None:
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_bucket = fake.name()
    dest_key = "test/destination.txt"
    object_storage_service.client.move.side_effect = OSError("Move failed")

    with pytest.raises(MoveFileError):
        object_storage_service.move_file(
            source_bucket=source_bucket,
            source_key=source_key,
            dest_bucket=dest_bucket,
            dest_key=dest_key,
        )
