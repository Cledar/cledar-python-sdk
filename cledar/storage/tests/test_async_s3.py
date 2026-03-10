# mypy: disable-error-code=no-untyped-def
import io
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from faker import Faker

from cledar.storage.async_object_storage import AsyncObjectStorageService
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

fake = Faker()


@pytest.fixture(name="async_object_storage_service")
@patch("fsspec.filesystem")
def fixture_async_object_storage_service(
    fsspec_client: MagicMock, object_storage_config: ObjectStorageServiceConfig
) -> AsyncObjectStorageService:
    mock_client = AsyncMock()
    mock_client.set_session = AsyncMock(return_value=AsyncMock())
    fsspec_client.return_value = mock_client
    service = AsyncObjectStorageService(object_storage_config)
    service.s3_client = mock_client
    service.local_client = MagicMock()
    return service


# Context Manager Tests


@pytest.mark.asyncio
async def test_context_manager_sets_and_closes_sessions(
    object_storage_config: ObjectStorageServiceConfig,
) -> None:
    """Test that context manager properly sets up and closes sessions."""
    with patch("fsspec.filesystem") as mock_filesystem:
        mock_s3_client = AsyncMock()
        mock_azure_client = AsyncMock()
        mock_s3_session = AsyncMock()
        mock_azure_session = AsyncMock()

        mock_s3_session.close = AsyncMock()
        mock_azure_session.close = AsyncMock()
        mock_s3_client.set_session = AsyncMock(return_value=mock_s3_session)
        mock_azure_client.set_session = AsyncMock(return_value=mock_azure_session)

        # Return different mocks for S3 and Azure
        mock_filesystem.side_effect = [mock_s3_client, mock_s3_client, mock_azure_client]

        service = AsyncObjectStorageService(object_storage_config)

        async with service as svc:
            assert svc is service
            # Both S3 and Azure sessions should be set
            mock_s3_client.set_session.assert_called_once()
            mock_azure_client.set_session.assert_called_once()

        # Both sessions should be closed
        mock_s3_session.close.assert_called_once()
        mock_azure_session.close.assert_called_once()


@pytest.mark.asyncio
async def test_context_manager_closes_sessions_on_exception(
    object_storage_config: ObjectStorageServiceConfig,
) -> None:
    """Test that context manager closes sessions even on exception."""
    with patch("fsspec.filesystem") as mock_filesystem:
        mock_s3_client = AsyncMock()
        mock_azure_client = AsyncMock()
        mock_s3_session = AsyncMock()
        mock_azure_session = AsyncMock()

        mock_s3_session.close = AsyncMock()
        mock_azure_session.close = AsyncMock()
        mock_s3_client.set_session = AsyncMock(return_value=mock_s3_session)
        mock_azure_client.set_session = AsyncMock(return_value=mock_azure_session)

        # Return different mocks for S3 and Azure
        mock_filesystem.side_effect = [mock_s3_client, mock_s3_client, mock_azure_client]

        service = AsyncObjectStorageService(object_storage_config)

        with pytest.raises(ValueError):
            async with service:
                raise ValueError("Test exception")

        # Both sessions should be closed even with exception
        mock_s3_session.close.assert_called_once()
        mock_azure_session.close.assert_called_once()


# Upload Buffer Tests


@pytest.mark.asyncio
async def test_upload_buffer_with_bucket_key_uses_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test that upload_buffer with bucket and key uses S3 client."""
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())
    bucket_name = fake.name()
    key = fake.name()

    async_object_storage_service.s3_client._pipe_file = AsyncMock()

    await async_object_storage_service.upload_buffer(
        buffer=buffer_bytes, bucket=bucket_name, key=key
    )

    async_object_storage_service.s3_client._pipe_file.assert_called_once()
    call_args = async_object_storage_service.s3_client._pipe_file.call_args
    assert f"s3://{bucket_name}/{key}" in str(call_args)


@pytest.mark.asyncio
async def test_upload_buffer_with_s3_path(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test upload_buffer with S3 path."""
    buffer_bytes = io.BytesIO(fake.text().encode())
    destination_path = f"s3://{fake.name()}/{fake.name()}"

    async_object_storage_service.s3_client._pipe_file = AsyncMock()

    await async_object_storage_service.upload_buffer(
        buffer=buffer_bytes, destination_path=destination_path
    )

    async_object_storage_service.s3_client._pipe_file.assert_called_once_with(
        path=destination_path, value=buffer_bytes.getvalue()
    )


@pytest.mark.asyncio
async def test_upload_buffer_with_abfs_path(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test upload_buffer with ABFS path."""
    buffer_bytes = io.BytesIO(fake.text().encode())
    destination_path = f"abfs://{fake.name()}/{fake.name()}"

    async_object_storage_service.azure_client = AsyncMock()
    async_object_storage_service.azure_client._pipe_file = AsyncMock()

    await async_object_storage_service.upload_buffer(
        buffer=buffer_bytes, destination_path=destination_path
    )

    async_object_storage_service.azure_client._pipe_file.assert_called_once_with(
        path=destination_path, value=buffer_bytes.getvalue()
    )


@pytest.mark.asyncio
async def test_upload_buffer_with_local_path(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test upload_buffer with local filesystem path."""
    buffer_bytes = io.BytesIO(fake.text().encode())
    destination_path = "/tmp/test_file.txt"

    mock_file = MagicMock()

    def open_cm(*_args, **_kwargs):
        from contextlib import contextmanager

        @contextmanager
        def _cm():
            yield mock_file

        return _cm()

    async_object_storage_service.local_client.open = MagicMock(side_effect=open_cm)

    await async_object_storage_service.upload_buffer(
        buffer=buffer_bytes, destination_path=destination_path
    )

    async_object_storage_service.local_client.open.assert_called_once_with(
        path=destination_path, mode="wb"
    )
    mock_file.write.assert_called_once()


@pytest.mark.asyncio
async def test_upload_buffer_exception(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test that upload_buffer raises UploadBufferError on failure."""
    buffer_bytes = io.BytesIO(fake.text().encode())
    bucket_name = fake.name()
    key = fake.name()

    async_object_storage_service.s3_client._pipe_file = AsyncMock(
        side_effect=OSError("Upload failed")
    )

    with pytest.raises(UploadBufferError):
        await async_object_storage_service.upload_buffer(
            buffer=buffer_bytes, bucket=bucket_name, key=key
        )


# Read File Tests


@pytest.mark.asyncio
async def test_read_file_with_bucket_key_uses_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test that read_file with bucket and key uses S3 client."""
    bucket_name = fake.name()
    key = fake.name()
    expected_content = fake.text().encode()

    async_object_storage_service.s3_client._cat_file = AsyncMock(
        return_value=expected_content
    )

    result = await async_object_storage_service.read_file(bucket=bucket_name, key=key)

    assert result == expected_content
    async_object_storage_service.s3_client._cat_file.assert_called_once_with(
        path=f"s3://{bucket_name}/{key}"
    )


@pytest.mark.asyncio
async def test_read_file_with_retry_success(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test read_file retries on failure and succeeds."""
    bucket_name = fake.name()
    key = fake.name()
    expected_content = fake.text().encode()

    async_object_storage_service.s3_client._cat_file = AsyncMock(
        side_effect=[OSError("Network error"), OSError("Network error"), expected_content]
    )

    with patch("asyncio.sleep", new_callable=AsyncMock):
        result = await async_object_storage_service.read_file(
            bucket=bucket_name, key=key, max_tries=3
        )

    assert result == expected_content
    assert async_object_storage_service.s3_client._cat_file.call_count == 3


@pytest.mark.asyncio
async def test_read_file_exception_after_retries(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test read_file raises ReadFileError after all retries fail."""
    bucket_name = fake.name()
    key = fake.name()

    async_object_storage_service.s3_client._cat_file = AsyncMock(
        side_effect=OSError("Network error")
    )

    with patch("asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(ReadFileError):
            await async_object_storage_service.read_file(
                bucket=bucket_name, key=key, max_tries=3
            )

    assert async_object_storage_service.s3_client._cat_file.call_count == 3


@pytest.mark.asyncio
async def test_read_file_with_exponential_backoff(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test read_file uses exponential backoff between retries."""
    bucket_name = fake.name()
    key = fake.name()
    expected_content = fake.text().encode()

    async_object_storage_service.s3_client._cat_file = AsyncMock(
        side_effect=[OSError("error"), OSError("error"), expected_content]
    )

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await async_object_storage_service.read_file(
            bucket=bucket_name, key=key, max_tries=3
        )

    assert mock_sleep.call_count == 2
    mock_sleep.assert_any_call(0.5)  # First retry: 0.5 * 1
    mock_sleep.assert_any_call(1.0)  # Second retry: 0.5 * 2


# Upload File Tests


@pytest.mark.asyncio
async def test_upload_file_with_bucket_key(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test upload_file with bucket and key."""
    file_path = fake.file_path()
    bucket_name = fake.name()
    key = fake.name()

    async_object_storage_service.s3_client._put_file = AsyncMock()

    await async_object_storage_service.upload_file(
        file_path=file_path, bucket=bucket_name, key=key
    )

    async_object_storage_service.s3_client._put_file.assert_called_once_with(
        lpath=file_path, rpath=f"s3://{bucket_name}/{key}"
    )


@pytest.mark.asyncio
async def test_upload_file_with_path(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test upload_file with destination path."""
    file_path = fake.file_path()
    destination_path = f"s3://{fake.name()}/{fake.name()}"

    async_object_storage_service.s3_client._put_file = AsyncMock()

    await async_object_storage_service.upload_file(
        file_path=file_path, destination_path=destination_path
    )

    async_object_storage_service.s3_client._put_file.assert_called_once_with(
        lpath=file_path, rpath=destination_path
    )


@pytest.mark.asyncio
async def test_upload_file_exception(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test upload_file raises UploadFileError on failure."""
    file_path = fake.file_path()
    bucket_name = fake.name()
    key = fake.name()

    async_object_storage_service.s3_client._put_file = AsyncMock(
        side_effect=OSError("Upload failed")
    )

    with pytest.raises(UploadFileError):
        await async_object_storage_service.upload_file(
            file_path=file_path, bucket=bucket_name, key=key
        )


# List Objects Tests


@pytest.mark.asyncio
async def test_list_objects_recursive_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test list_objects with recursive=True for S3."""
    bucket = fake.name()
    prefix = "test/prefix"
    mock_objects = [
        f"s3://{bucket}/{prefix}/file1.txt",
        f"s3://{bucket}/{prefix}/file2.txt",
        f"s3://{bucket}/{prefix}/subfolder/file3.txt",
    ]
    async_object_storage_service.s3_client._find = AsyncMock(return_value=mock_objects)

    result = await async_object_storage_service.list_objects(
        bucket=bucket, prefix=prefix, recursive=True
    )

    assert len(result) == 3
    assert f"{prefix}/file1.txt" in result
    assert f"{prefix}/file2.txt" in result
    assert f"{prefix}/subfolder/file3.txt" in result


@pytest.mark.asyncio
async def test_list_objects_non_recursive_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test list_objects with recursive=False for S3."""
    bucket = fake.name()
    prefix = "test/prefix"
    mock_objects = [
        f"s3://{bucket}/{prefix}/file1.txt",
        f"s3://{bucket}/{prefix}/file2.txt",
    ]
    async_object_storage_service.s3_client._ls = AsyncMock(return_value=mock_objects)

    result = await async_object_storage_service.list_objects(
        bucket=bucket, prefix=prefix, recursive=False
    )

    assert len(result) == 2
    assert f"{prefix}/file1.txt" in result
    assert f"{prefix}/file2.txt" in result


@pytest.mark.asyncio
async def test_list_objects_with_path(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test list_objects with path parameter."""
    path = f"s3://{fake.name()}/test/"
    mock_objects = [f"{path}file1.txt", f"{path}file2.txt"]
    async_object_storage_service.s3_client._find = AsyncMock(return_value=mock_objects)

    result = await async_object_storage_service.list_objects(path=path, recursive=True)

    assert len(result) == 2
    async_object_storage_service.s3_client._find.assert_called_once()


@pytest.mark.asyncio
async def test_list_objects_exception(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test list_objects raises ListObjectsError on failure."""
    bucket = fake.name()
    async_object_storage_service.s3_client._find = AsyncMock(
        side_effect=OSError("List failed")
    )

    with pytest.raises(ListObjectsError):
        await async_object_storage_service.list_objects(bucket=bucket)


# Delete File Tests


@pytest.mark.asyncio
async def test_delete_file_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test delete_file for S3."""
    bucket = fake.name()
    key = "test/file.txt"

    async_object_storage_service.s3_client._rm = AsyncMock()

    await async_object_storage_service.delete_file(bucket=bucket, key=key)

    async_object_storage_service.s3_client._rm.assert_called_once_with(
        f"s3://{bucket}/{key}"
    )


@pytest.mark.asyncio
async def test_delete_file_abfs(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test delete_file for ABFS."""
    path = f"abfs://{fake.name()}/test/file.txt"

    async_object_storage_service.azure_client = AsyncMock()
    async_object_storage_service.azure_client._rm = AsyncMock()

    await async_object_storage_service.delete_file(path=path)

    async_object_storage_service.azure_client._rm.assert_called_once_with(path)


@pytest.mark.asyncio
async def test_delete_file_exception(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test delete_file raises DeleteFileError on failure."""
    bucket = fake.name()
    key = "test/file.txt"
    async_object_storage_service.s3_client._rm = AsyncMock(
        side_effect=OSError("Delete failed")
    )

    with pytest.raises(DeleteFileError):
        await async_object_storage_service.delete_file(bucket=bucket, key=key)


# File Exists Tests


@pytest.mark.asyncio
async def test_file_exists_true_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test file_exists returns True for existing S3 file."""
    bucket = fake.name()
    key = "test/file.txt"
    async_object_storage_service.s3_client._exists = AsyncMock(return_value=True)

    result = await async_object_storage_service.file_exists(bucket=bucket, key=key)

    assert result is True
    async_object_storage_service.s3_client._exists.assert_called_once_with(
        f"s3://{bucket}/{key}"
    )


@pytest.mark.asyncio
async def test_file_exists_false_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test file_exists returns False for non-existing S3 file."""
    bucket = fake.name()
    key = "test/file.txt"
    async_object_storage_service.s3_client._exists = AsyncMock(return_value=False)

    result = await async_object_storage_service.file_exists(bucket=bucket, key=key)

    assert result is False
    async_object_storage_service.s3_client._exists.assert_called_once_with(
        f"s3://{bucket}/{key}"
    )


@pytest.mark.asyncio
async def test_file_exists_abfs(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test file_exists for ABFS path."""
    path = f"abfs://{fake.name()}/test/file.txt"

    async_object_storage_service.azure_client = AsyncMock()
    async_object_storage_service.azure_client._exists = AsyncMock(return_value=True)

    result = await async_object_storage_service.file_exists(path=path)

    assert result is True
    async_object_storage_service.azure_client._exists.assert_called_once_with(path)


@pytest.mark.asyncio
async def test_file_exists_exception(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test file_exists raises CheckFileExistenceError on failure."""
    bucket = fake.name()
    key = "test/file.txt"
    async_object_storage_service.s3_client._exists = AsyncMock(
        side_effect=OSError("Check failed")
    )

    with pytest.raises(CheckFileExistenceError):
        await async_object_storage_service.file_exists(bucket=bucket, key=key)


# Download File Tests


@pytest.mark.asyncio
async def test_download_file_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test download_file for S3."""
    bucket = fake.name()
    key = "test/file.txt"
    dest_path = "/tmp/downloaded_file.txt"

    async_object_storage_service.s3_client._get_file = AsyncMock()

    await async_object_storage_service.download_file(
        dest_path, bucket=bucket, key=key
    )

    async_object_storage_service.s3_client._get_file.assert_called_once_with(
        rpath=f"s3://{bucket}/{key}", lpath=dest_path
    )


@pytest.mark.asyncio
async def test_download_file_with_retry_success(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test download_file retries on failure and succeeds."""
    bucket = fake.name()
    key = "test/file.txt"
    dest_path = "/tmp/downloaded_file.txt"

    async_object_storage_service.s3_client._get_file = AsyncMock(
        side_effect=[OSError("Network error"), OSError("Network error"), None]
    )

    with patch("asyncio.sleep", new_callable=AsyncMock):
        await async_object_storage_service.download_file(
            dest_path, bucket=bucket, key=key, max_tries=3
        )

    assert async_object_storage_service.s3_client._get_file.call_count == 3


@pytest.mark.asyncio
async def test_download_file_exception_after_retries(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test download_file raises DownloadFileError after all retries fail."""
    bucket = fake.name()
    key = "test/file.txt"
    dest_path = "/tmp/downloaded_file.txt"
    async_object_storage_service.s3_client._get_file = AsyncMock(
        side_effect=OSError("Network error")
    )

    with patch("asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(DownloadFileError):
            await async_object_storage_service.download_file(
                dest_path, bucket=bucket, key=key, max_tries=3
            )

    assert async_object_storage_service.s3_client._get_file.call_count == 3


@pytest.mark.asyncio
async def test_download_file_with_exponential_backoff(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test download_file uses exponential backoff between retries."""
    bucket = fake.name()
    key = "test/file.txt"
    dest_path = "/tmp/downloaded_file.txt"

    async_object_storage_service.s3_client._get_file = AsyncMock(
        side_effect=[OSError("error"), OSError("error"), None]
    )

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await async_object_storage_service.download_file(
            dest_path, bucket=bucket, key=key, max_tries=3
        )

    assert mock_sleep.call_count == 2
    mock_sleep.assert_any_call(0.5)
    mock_sleep.assert_any_call(1.0)


# Get File Size Tests


@pytest.mark.asyncio
async def test_get_file_size_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test get_file_size for S3."""
    bucket = fake.name()
    key = "test/file.txt"
    expected_size = 1024
    async_object_storage_service.s3_client._info = AsyncMock(
        return_value={"size": expected_size}
    )

    result = await async_object_storage_service.get_file_size(bucket=bucket, key=key)

    assert result == expected_size
    async_object_storage_service.s3_client._info.assert_called_once_with(
        f"s3://{bucket}/{key}"
    )


@pytest.mark.asyncio
async def test_get_file_size_abfs(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test get_file_size for ABFS."""
    path = f"abfs://{fake.name()}/test/file.txt"
    expected_size = 2048

    async_object_storage_service.azure_client = AsyncMock()
    async_object_storage_service.azure_client._info = AsyncMock(
        return_value={"size": expected_size}
    )

    result = await async_object_storage_service.get_file_size(path=path)

    assert result == expected_size
    async_object_storage_service.azure_client._info.assert_called_once_with(path)


@pytest.mark.asyncio
async def test_get_file_size_exception(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test get_file_size raises GetFileSizeError on failure."""
    bucket = fake.name()
    key = "test/file.txt"
    async_object_storage_service.s3_client._info = AsyncMock(
        side_effect=OSError("Info failed")
    )

    with pytest.raises(GetFileSizeError):
        await async_object_storage_service.get_file_size(bucket=bucket, key=key)


# Get File Info Tests


@pytest.mark.asyncio
async def test_get_file_info_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test get_file_info for S3."""
    bucket = fake.name()
    key = "test/file.txt"
    expected_info = {
        "size": 1024,
        "LastModified": "2025-01-01T00:00:00Z",
        "ContentType": "text/plain",
    }
    async_object_storage_service.s3_client._info = AsyncMock(return_value=expected_info)

    result = await async_object_storage_service.get_file_info(bucket=bucket, key=key)

    assert result == expected_info
    async_object_storage_service.s3_client._info.assert_called_once_with(
        f"s3://{bucket}/{key}"
    )


@pytest.mark.asyncio
async def test_get_file_info_abfs(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test get_file_info for ABFS."""
    path = f"abfs://{fake.name()}/test/file.txt"
    expected_info = {"size": 2048, "type": "file"}

    async_object_storage_service.azure_client = AsyncMock()
    async_object_storage_service.azure_client._info = AsyncMock(
        return_value=expected_info
    )

    result = await async_object_storage_service.get_file_info(path=path)

    assert result == expected_info
    async_object_storage_service.azure_client._info.assert_called_once_with(path)


@pytest.mark.asyncio
async def test_get_file_info_exception(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test get_file_info raises GetFileInfoError on failure."""
    bucket = fake.name()
    key = "test/file.txt"
    async_object_storage_service.s3_client._info = AsyncMock(
        side_effect=OSError("Info failed")
    )

    with pytest.raises(GetFileInfoError):
        await async_object_storage_service.get_file_info(bucket=bucket, key=key)


# Copy File Tests


@pytest.mark.asyncio
async def test_copy_file_s3_to_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test copy_file from S3 to S3."""
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_bucket = fake.name()
    dest_key = "test/destination.txt"

    async_object_storage_service.s3_client._copy = AsyncMock()

    await async_object_storage_service.copy_file(
        source_bucket=source_bucket,
        source_key=source_key,
        dest_bucket=dest_bucket,
        dest_key=dest_key,
    )

    async_object_storage_service.s3_client._copy.assert_called_once_with(
        path1=f"s3://{source_bucket}/{source_key}",
        path2=f"s3://{dest_bucket}/{dest_key}",
    )


@pytest.mark.asyncio
async def test_copy_file_s3_to_local(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test copy_file from S3 to local filesystem."""
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_path = "/tmp/dest/file.txt"

    async_object_storage_service.s3_client._copy = AsyncMock()

    await async_object_storage_service.copy_file(
        source_bucket=source_bucket, source_key=source_key, dest_path=dest_path
    )

    async_object_storage_service.s3_client._copy.assert_called_once_with(
        path1=f"s3://{source_bucket}/{source_key}", path2=dest_path
    )


@pytest.mark.asyncio
async def test_copy_file_mixed_backend(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test copy_file between different backends (S3 to ABFS)."""
    source_path = f"s3://{fake.name()}/source.txt"
    dest_path = f"abfs://{fake.name()}/dest.txt"
    test_data = b"test data"

    async_object_storage_service.s3_client._cat_file = AsyncMock(return_value=test_data)
    async_object_storage_service.azure_client = AsyncMock()
    async_object_storage_service.azure_client._pipe_file = AsyncMock()

    await async_object_storage_service.copy_file(
        source_path=source_path, dest_path=dest_path
    )

    async_object_storage_service.s3_client._cat_file.assert_called_once_with(source_path)
    async_object_storage_service.azure_client._pipe_file.assert_called_once_with(
        dest_path, test_data
    )


@pytest.mark.asyncio
async def test_copy_file_exception(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test copy_file raises CopyFileError on failure."""
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_bucket = fake.name()
    dest_key = "test/destination.txt"
    async_object_storage_service.s3_client._copy = AsyncMock(
        side_effect=OSError("Copy failed")
    )

    with pytest.raises(CopyFileError):
        await async_object_storage_service.copy_file(
            source_bucket=source_bucket,
            source_key=source_key,
            dest_bucket=dest_bucket,
            dest_key=dest_key,
        )


# Move File Tests


@pytest.mark.asyncio
async def test_move_file_s3_to_s3(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test move_file from S3 to S3."""
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_bucket = fake.name()
    dest_key = "test/destination.txt"

    async_object_storage_service.s3_client._mv = AsyncMock()

    await async_object_storage_service.move_file(
        source_bucket=source_bucket,
        source_key=source_key,
        dest_bucket=dest_bucket,
        dest_key=dest_key,
    )

    async_object_storage_service.s3_client._mv.assert_called_once_with(
        path1=f"s3://{source_bucket}/{source_key}",
        path2=f"s3://{dest_bucket}/{dest_key}",
    )


@pytest.mark.asyncio
async def test_move_file_s3_to_local(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test move_file from S3 to local filesystem."""
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_path = "/tmp/dest/file.txt"

    async_object_storage_service.s3_client._mv = AsyncMock()

    await async_object_storage_service.move_file(
        source_bucket=source_bucket, source_key=source_key, dest_path=dest_path
    )

    async_object_storage_service.s3_client._mv.assert_called_once_with(
        path1=f"s3://{source_bucket}/{source_key}", path2=dest_path
    )


@pytest.mark.asyncio
async def test_move_file_mixed_backend(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test move_file between different backends (S3 to ABFS)."""
    source_path = f"s3://{fake.name()}/source.txt"
    dest_path = f"abfs://{fake.name()}/dest.txt"
    test_data = b"test data"

    async_object_storage_service.s3_client._cat_file = AsyncMock(return_value=test_data)
    async_object_storage_service.s3_client._rm = AsyncMock()
    async_object_storage_service.azure_client = AsyncMock()
    async_object_storage_service.azure_client._pipe_file = AsyncMock()

    await async_object_storage_service.move_file(
        source_path=source_path, dest_path=dest_path
    )

    async_object_storage_service.s3_client._cat_file.assert_called_once()
    async_object_storage_service.azure_client._pipe_file.assert_called_once()
    async_object_storage_service.s3_client._rm.assert_called_once_with(source_path)


@pytest.mark.asyncio
async def test_move_file_exception(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test move_file raises MoveFileError on failure."""
    source_bucket = fake.name()
    source_key = "test/source.txt"
    dest_bucket = fake.name()
    dest_key = "test/destination.txt"
    async_object_storage_service.s3_client._mv = AsyncMock(
        side_effect=OSError("Move failed")
    )

    with pytest.raises(MoveFileError):
        await async_object_storage_service.move_file(
            source_bucket=source_bucket,
            source_key=source_key,
            dest_bucket=dest_bucket,
            dest_key=dest_key,
        )


# Has Bucket Tests


@pytest.mark.asyncio
async def test_has_bucket_exists(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test has_bucket returns True for existing bucket."""
    bucket_name = fake.name()
    async_object_storage_service.s3_client._ls = AsyncMock(return_value=[])

    result = await async_object_storage_service.has_bucket(bucket=bucket_name)

    assert result is True
    async_object_storage_service.s3_client._ls.assert_called_once_with(path=bucket_name)


@pytest.mark.asyncio
async def test_has_bucket_not_exists(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test has_bucket returns False for non-existing bucket."""
    bucket_name = fake.name()
    async_object_storage_service.s3_client._ls = AsyncMock(
        side_effect=OSError("Bucket not found")
    )

    result = await async_object_storage_service.has_bucket(bucket=bucket_name)

    assert result is False


@pytest.mark.asyncio
async def test_has_bucket_throw_exception(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test has_bucket raises exception when throw=True."""
    bucket_name = fake.name()
    async_object_storage_service.s3_client._ls = AsyncMock(
        side_effect=OSError("Bucket not found")
    )

    with pytest.raises(RequiredBucketNotFoundError):
        await async_object_storage_service.has_bucket(bucket=bucket_name, throw=True)


# Is Alive Tests


@pytest.mark.asyncio
async def test_is_alive_true(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test is_alive returns True when service is accessible."""
    async_object_storage_service.s3_client._ls = AsyncMock(return_value=[])

    result = await async_object_storage_service.is_alive()

    assert result is True


@pytest.mark.asyncio
async def test_is_alive_false(
    async_object_storage_service: AsyncObjectStorageService,
) -> None:
    """Test is_alive returns False when service is not accessible."""
    async_object_storage_service.s3_client._ls = AsyncMock(
        side_effect=OSError("Connection failed")
    )

    result = await async_object_storage_service.is_alive()

    assert result is False
