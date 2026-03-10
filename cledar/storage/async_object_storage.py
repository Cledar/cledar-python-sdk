"""Asynchronous service for interacting with S3, ABFS, and local filesystem storage."""

import asyncio
import io
import logging
from typing import Any, cast

import fsspec
from fsspec.exceptions import FSTimeoutError

from .base_object_storage import BaseObjectStorageService
from .constants import S3_PATH_PREFIX
from .exceptions import (
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
from .models import ObjectStorageServiceConfig, TransferPath

logger = logging.getLogger("async_object_storage_service")


async def _put_file(fs: Any, lpath: str, rpath: str) -> None:
    """Upload local file to remote storage.

    Args:
        fs: Filesystem client.
        lpath: Local file path.
        rpath: Remote file path.

    """
    if hasattr(fs, "_put_file"):
        await fs._put_file(lpath=lpath, rpath=rpath)
    else:
        # Fallback for local filesystem
        fs.put(lpath=lpath, rpath=rpath)


async def _get_file(fs: Any, src: str, dst: str) -> None:
    """Download remote file to local storage.

    Args:
        fs: Filesystem client.
        src: Remote source path.
        dst: Local destination path.

    """
    if hasattr(fs, "_get_file"):
        await fs._get_file(rpath=src, lpath=dst)
    else:
        # Fallback for local filesystem
        fs.get(src, dst)


async def _list_via_find_or_ls(fs: Any, path: str, recursive: bool) -> list[str]:
    """List files using find (recursive) or ls (non-recursive).

    Args:
        fs: Filesystem client.
        path: Path to list.
        recursive: Whether to list recursively.

    Returns:
        list[str]: List of file paths.

    """
    if recursive:
        if hasattr(fs, "_find"):
            return cast(list[str], await fs._find(path))
        return cast(list[str], fs.find(path))
    if hasattr(fs, "_ls"):
        return cast(list[str], await fs._ls(path, detail=False))
    return cast(list[str], fs.ls(path, detail=False))


class AsyncObjectStorageService(BaseObjectStorageService):
    """Asynchronous service for managing object storage operations.

    Supports multiple backends.
    """

    _s3_session: Any = None
    _azure_session: Any = None

    def __init__(self, config: ObjectStorageServiceConfig) -> None:
        """Initialize the async object storage service.

        Args:
            config: Configuration for S3 and Azure storage backends.

        """
        self.config = config
        if config.s3_endpoint_url:
            self.s3_client = fsspec.filesystem(
                "s3",
                key=config.s3_access_key,
                secret=config.s3_secret_key,
                client_kwargs={"endpoint_url": config.s3_endpoint_url},
                max_concurrency=config.s3_max_concurrency,
                asynchronous=True,
            )
        else:
            self.s3_client = None
        self.local_client = fsspec.filesystem("file")

        if config.azure_account_name and config.azure_account_key:
            self.azure_client = fsspec.filesystem(
                "abfs",
                account_name=config.azure_account_name,
                account_key=config.azure_account_key,
                asynchronous=True,
            )
        else:
            self.azure_client = None
        logger.info(
            "Available filesystems",
            extra={
                "s3": self.s3_client is not None,
                "azure": self.azure_client is not None,
                "local": self.local_client is not None,
            },
        )

    async def __aenter__(self) -> "AsyncObjectStorageService":
        """Enter async context manager.

        Returns:
            AsyncObjectStorageService: Self instance with active sessions.

        """
        self._s3_session = await self.s3_client.set_session()
        if self.azure_client:
            self._azure_session = await self.azure_client.set_session()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit async context manager and close sessions.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.

        """
        if self._s3_session:
            await self._s3_session.close()
        if self._azure_session:
            await self._azure_session.close()

    async def is_alive(self) -> bool:
        """Check if the storage service is accessible.

        Returns:
            bool: True if the service is accessible, False otherwise.

        """
        try:
            if self.s3_client:
                await self.s3_client._ls(path="")
            elif self.azure_client:
                await self.azure_client._ls(path="")
            elif self.local_client:
                await self.local_client._ls(path="")
            else:
                return False
            return True
        except (OSError, PermissionError, TimeoutError, FSTimeoutError):
            return False

    async def _write_buffer_to_s3_key(
        self, buffer: io.BytesIO, bucket: str, key: str
    ) -> None:
        """Write buffer to S3 using bucket and key.

        Args:
            buffer: Buffer containing data to write.
            bucket: S3 bucket name.
            key: S3 object key.

        """
        buffer.seek(0)
        data = buffer.getvalue()
        await self.s3_client._pipe_file(
            path=f"{S3_PATH_PREFIX}{bucket}/{key}", value=data
        )

    async def _write_buffer_to_s3_path(
        self, buffer: io.BytesIO, destination_path: str
    ) -> None:
        """Write buffer to S3 using full path.

        Args:
            buffer: Buffer containing data to write.
            destination_path: Full S3 path (e.g., s3://bucket/key).

        """
        buffer.seek(0)
        data = buffer.getvalue()
        await self.s3_client._pipe_file(path=destination_path, value=data)

    async def _write_buffer_to_abfs_path(
        self, buffer: io.BytesIO, destination_path: str
    ) -> None:
        """Write buffer to Azure Blob Storage.

        Args:
            buffer: Buffer containing data to write.
            destination_path: Full ABFS path (e.g., abfs://container/path).

        """
        buffer.seek(0)
        data = buffer.getvalue()
        await self.azure_client._pipe_file(path=destination_path, value=data)

    def _write_buffer_to_local_path(
        self, buffer: io.BytesIO, destination_path: str
    ) -> None:
        """Write buffer to local filesystem (synchronous).

        Args:
            buffer: Buffer containing data to write.
            destination_path: Local filesystem path.

        """
        buffer.seek(0)
        with self.local_client.open(path=destination_path, mode="wb") as fobj:
            fobj.write(buffer.getbuffer())

    async def _read_from_s3_key(self, bucket: str, key: str) -> bytes:
        """Read file from S3 using bucket and key.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.

        Returns:
            bytes: File contents as bytes.

        """
        data: bytes = await self.s3_client._cat_file(
            path=f"{S3_PATH_PREFIX}{bucket}/{key}"
        )
        return data

    async def _read_from_s3_path(self, path: str) -> bytes:
        """Read file from S3 using full path.

        Args:
            path: Full S3 path (e.g., s3://bucket/key).

        Returns:
            bytes: File contents as bytes.

        """
        data: bytes = await self.s3_client._cat_file(path=path)
        return data

    async def _read_from_abfs_path(self, path: str) -> bytes:
        """Read file from Azure Blob Storage.

        Args:
            path: Full ABFS path (e.g., abfs://container/path).

        Returns:
            bytes: File contents as bytes.

        """
        data: bytes = await self.azure_client._cat_file(path=path)
        return data

    def _read_from_local_path(self, path: str) -> bytes:
        """Read file from local filesystem (synchronous).

        Args:
            path: Local filesystem path.

        Returns:
            bytes: File contents as bytes.

        """
        with self.local_client.open(path=path, mode="rb") as fobj:
            data: bytes = fobj.read()
            return data

    async def _copy_with_backend(self, backend: str, src: str, dst: str) -> None:
        """Copy file using the appropriate backend.

        Args:
            backend: Backend type (s3, abfs, local, or mixed).
            src: Source path.
            dst: Destination path.

        """
        if backend == "s3":
            await self.s3_client._copy(path1=src, path2=dst)
            return
        if backend == "abfs":
            await self.azure_client._copy(path1=src, path2=dst)
            return
        if backend == "local":
            self.local_client.copy(src, dst)
            return
        # Mixed backend: read from source, write to destination
        if self._is_s3_path(src):
            data = await self.s3_client._cat_file(src)
        elif self._is_abfs_path(src):
            data = await self.azure_client._cat_file(src)
        else:
            data = self._read_from_local_path(src)

        if self._is_s3_path(dst):
            await self.s3_client._pipe_file(dst, data)
        elif self._is_abfs_path(dst):
            await self.azure_client._pipe_file(dst, data)
        else:
            with self.local_client.open(dst, mode="wb") as f:
                f.write(data)

    async def _move_with_backend(self, backend: str, src: str, dst: str) -> None:
        """Move file using the appropriate backend.

        Args:
            backend: Backend type (s3, abfs, local, or mixed).
            src: Source path.
            dst: Destination path.

        """
        if backend == "s3":
            await self.s3_client._mv(path1=src, path2=dst)
            return
        if backend == "abfs":
            await self.azure_client._mv(path1=src, path2=dst)
            return
        if backend == "local":
            self.local_client.move(src, dst)
            return
        # Mixed backend: copy then delete
        await self._copy_with_backend("mixed", src, dst)
        if self._is_s3_path(src):
            await self.s3_client._rm(src)
        elif self._is_abfs_path(src):
            await self.azure_client._rm(src)
        else:
            self.local_client.rm(src)

    async def _read_from_backend_path(self, backend: str, src_path: str) -> bytes:
        """Read file from the specified backend.

        Args:
            backend: Backend type (s3, abfs, or local).
            src_path: Source path to read from.

        Returns:
            bytes: File contents as bytes.

        """
        if backend == "s3":
            return await self._read_from_s3_path(src_path)
        if backend == "abfs":
            return await self._read_from_abfs_path(src_path)
        return self._read_from_local_path(src_path)

    async def has_bucket(self, bucket: str, throw: bool = False) -> bool:
        """Check if an S3 bucket exists and is accessible.

        Args:
            bucket: S3 bucket name.
            throw: Whether to raise an exception if bucket is not found.

        Returns:
            bool: True if bucket exists and is accessible, False otherwise.

        Raises:
            RequiredBucketNotFoundError: If throw=True and bucket is not found.

        """
        try:
            if self.s3_client:
                await self.s3_client._ls(path=bucket)
            elif self.azure_client:
                await self.azure_client._ls(path=bucket)
            elif self.local_client:
                await self.local_client._ls(path=bucket)
            else:
                return False
            return True
        except (
            FileNotFoundError,
            PermissionError,
            OSError,
            TimeoutError,
            FSTimeoutError,
        ) as exception:
            if throw:
                logger.exception("Bucket not found", extra={"bucket": bucket})
                raise RequiredBucketNotFoundError from exception
            return False

    async def upload_buffer(
        self,
        buffer: io.BytesIO,
        bucket: str | None = None,
        key: str | None = None,
        destination_path: str | None = None,
    ) -> None:
        """Upload a buffer to storage.

        Args:
            buffer: Buffer containing data to upload.
            bucket: S3 bucket name (for S3 destination).
            key: S3 object key (for S3 destination).
            destination_path: Full destination path (can be S3, ABFS, or local).

        Raises:
            UploadBufferError: If upload fails.
            ValueError: If neither destination_path nor bucket+key are provided.

        """
        try:
            if bucket and key:
                await self._write_buffer_to_s3_key(
                    buffer=buffer, bucket=bucket, key=key
                )
                logger.debug(
                    "Uploaded file from buffer", extra={"bucket": bucket, "key": key}
                )
            elif destination_path and self._is_s3_path(destination_path):
                logger.debug(
                    "Uploading file from buffer to S3 via path",
                    extra={"destination_path": destination_path},
                )
                await self._write_buffer_to_s3_path(
                    buffer=buffer, destination_path=destination_path
                )
            elif destination_path and self._is_abfs_path(destination_path):
                logger.debug(
                    "Uploading file from buffer to ABFS via path",
                    extra={"destination_path": destination_path},
                )
                await self._write_buffer_to_abfs_path(
                    buffer=buffer, destination_path=destination_path
                )
            elif destination_path:
                logger.debug(
                    "Uploading file from buffer to local filesystem",
                    extra={"destination_path": destination_path},
                )
                self._write_buffer_to_local_path(
                    buffer=buffer, destination_path=destination_path
                )
            else:
                raise ValueError(
                    "Either destination_path or bucket and key must be provided"
                )
        except (OSError, PermissionError, TimeoutError, FSTimeoutError) as exception:
            logger.exception(
                "Failed to upload buffer",
                extra={
                    "bucket": bucket,
                    "key": key,
                    "destination_path": destination_path,
                },
            )
            raise UploadBufferError(
                f"Failed to upload buffer (bucket={bucket}, key={key}, "
                f"destination_path={destination_path})"
            ) from exception

    async def read_file(
        self,
        bucket: str | None = None,
        key: str | None = None,
        path: str | None = None,
        max_tries: int = 3,
    ) -> bytes:
        """Read file from storage.

        Args:
            bucket: S3 bucket name (for S3 source).
            key: S3 object key (for S3 source).
            path: Full source path (can be S3, ABFS, or local).
            max_tries: Number of retry attempts on failure.

        Returns:
            bytes: File contents as bytes.

        Raises:
            ReadFileError: If read fails after all retries.
            NotImplementedError: If this should never be reached.

        """
        transfer_path: TransferPath = self._resolve_source_backend_and_path(
            bucket=bucket, key=key, path=path
        )
        backend_name: str = transfer_path.backend
        src_path: str = transfer_path.path
        for attempt in range(max_tries):
            try:
                logger.debug(
                    "Reading file",
                    extra={"backend": backend_name, "source": src_path},
                )
                content = await self._read_from_backend_path(backend_name, src_path)
                logger.debug(
                    "File read",
                    extra={"backend": backend_name, "source": src_path},
                )
                return content
            except OSError as exception:
                if attempt == max_tries - 1:
                    logger.exception(
                        "Failed to read file after %d retries",
                        max_tries,
                        extra={"bucket": bucket, "key": key, "path": path},
                    )
                    raise ReadFileError(
                        f"Failed to read file after {max_tries} retries "
                        f"(bucket={bucket}, key={key}, path={path})"
                    ) from exception
                logger.warning(
                    "Failed to read file, retrying...",
                    extra={"attempt": attempt + 1},
                )
                await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
        raise NotImplementedError("This should never be reached")

    async def upload_file(
        self,
        file_path: str,
        bucket: str | None = None,
        key: str | None = None,
        destination_path: str | None = None,
    ) -> None:
        """Upload a local file to storage.

        Args:
            file_path: Local file path to upload.
            bucket: S3 bucket name (for S3 destination).
            key: S3 object key (for S3 destination).
            destination_path: Full destination path (can be S3, ABFS, or local).

        Raises:
            UploadFileError: If upload fails.

        """
        try:
            transfer_path: TransferPath = self._resolve_dest_backend_and_path(
                bucket=bucket, key=key, destination_path=destination_path
            )
            backend_name: str = transfer_path.backend
            dst_path: str = transfer_path.path
            logger.debug(
                "Uploading file",
                extra={
                    "backend": backend_name,
                    "destination": dst_path,
                    "file": file_path,
                },
            )
            fs = self._get_fs_for_backend(backend_name)
            await _put_file(fs, lpath=file_path, rpath=dst_path)
            logger.debug(
                "Uploaded file",
                extra={
                    "backend": backend_name,
                    "destination": dst_path,
                    "file": file_path,
                },
            )
        except (OSError, PermissionError, TimeoutError, FSTimeoutError) as exception:
            logger.exception(
                "Failed to upload file",
                extra={
                    "bucket": bucket,
                    "key": key,
                    "destination_path": destination_path,
                    "file_path": file_path,
                },
            )
            raise UploadFileError(
                f"Failed to upload file {file_path} "
                f"(bucket={bucket}, key={key}, destination_path={destination_path})"
            ) from exception

    async def list_objects(
        self,
        bucket: str | None = None,
        prefix: str = "",
        path: str | None = None,
        recursive: bool = True,
    ) -> list[str]:
        """List objects in storage with optional prefix filtering.

        Args:
            bucket: The bucket name (for S3)
            prefix: Optional prefix to filter objects (for S3)
            path: The filesystem path. Uses S3 if starts with s3://, otherwise local
            recursive: If True, list all objects recursively

        Returns:
            list[str]: List of object keys/paths

        Raises:
            ListObjectsError: If listing objects fails.
            ValueError: If neither path nor bucket are provided.

        """
        try:
            if path:
                transfer_path: TransferPath = self._resolve_path_backend(path)
                backend_name: str = transfer_path.backend
                resolved_path: str = transfer_path.path
                logger.debug(
                    "Listing objects",
                    extra={
                        "backend": backend_name,
                        "path": resolved_path,
                        "recursive": recursive,
                    },
                )
                fs = self._get_fs_for_backend(backend_name)
                objects = await _list_via_find_or_ls(fs, resolved_path, recursive)
                logger.debug(
                    "Listed objects",
                    extra={
                        "backend": backend_name,
                        "path": resolved_path,
                        "count": len(objects),
                    },
                )
                return objects
            if bucket:
                s3_path = (
                    f"{S3_PATH_PREFIX}{bucket}/{prefix}"
                    if prefix
                    else f"{S3_PATH_PREFIX}{bucket}/"
                )
                logger.debug(
                    "Listing objects from S3",
                    extra={"bucket": bucket, "prefix": prefix, "recursive": recursive},
                )
                objects = await _list_via_find_or_ls(self.s3_client, s3_path, recursive)
                keys = self._normalize_s3_keys(bucket, objects)
                logger.debug(
                    "Listed objects from S3",
                    extra={
                        "bucket": bucket,
                        "prefix": prefix,
                        "count": len(keys),
                    },
                )
                return keys
            raise ValueError("Either path or bucket must be provided")
        except (
            FileNotFoundError,
            PermissionError,
            OSError,
            TimeoutError,
            FSTimeoutError,
        ) as exception:
            logger.exception(
                "Failed to list objects",
                extra={"bucket": bucket, "prefix": prefix, "path": path},
            )
            raise ListObjectsError(
                f"Failed to list objects (bucket={bucket}, prefix={prefix}, "
                f"path={path})"
            ) from exception

    async def delete_file(
        self, bucket: str | None = None, key: str | None = None, path: str | None = None
    ) -> None:
        """Delete a single object from storage.

        Args:
            bucket: The bucket name (for S3)
            key: The object key to delete (for S3)
            path: The filesystem path. Uses S3 if starts with s3://, otherwise local

        Raises:
            DeleteFileError: If deleting the file fails.
            ValueError: If neither path nor bucket+key are provided.

        """
        try:
            if bucket and key:
                s3_path = f"{S3_PATH_PREFIX}{bucket}/{key}"
                logger.debug(
                    "Deleting file from S3", extra={"bucket": bucket, "key": key}
                )
                await self.s3_client._rm(s3_path)
                logger.debug(
                    "Deleted file from S3", extra={"bucket": bucket, "key": key}
                )
            elif path and self._is_s3_path(path):
                logger.debug("Deleting file from S3 via path", extra={"path": path})
                await self.s3_client._rm(path)
                logger.debug("Deleted file from S3 via path", extra={"path": path})
            elif path and self._is_abfs_path(path):
                logger.debug("Deleting file from ABFS via path", extra={"path": path})
                await self.azure_client._rm(path)
                logger.debug("Deleted file from ABFS via path", extra={"path": path})
            elif path:
                logger.debug(
                    "Deleting file from local filesystem", extra={"path": path}
                )
                self.local_client.rm(path)
                logger.debug("Deleted file from local filesystem", extra={"path": path})
            else:
                raise ValueError("Either path or bucket and key must be provided")
        except (
            FileNotFoundError,
            PermissionError,
            OSError,
            TimeoutError,
            FSTimeoutError,
        ) as exception:
            logger.exception(
                "Failed to delete file",
                extra={"bucket": bucket, "key": key, "path": path},
            )
            raise DeleteFileError(
                f"Failed to delete file (bucket={bucket}, key={key}, path={path})"
            ) from exception

    async def file_exists(
        self, bucket: str | None = None, key: str | None = None, path: str | None = None
    ) -> bool:
        """Check if a specific file exists in storage.

        Args:
            bucket: The bucket name (for S3)
            key: The object key to check (for S3)
            path: The filesystem path. Uses S3 if starts with s3://, otherwise local

        Returns:
            bool: True if the file exists, False otherwise

        Raises:
            CheckFileExistenceError: If checking file existence fails.
            ValueError: If neither path nor bucket+key are provided.

        """
        try:
            if bucket and key:
                s3_path = f"{S3_PATH_PREFIX}{bucket}/{key}"
                exists = await self.s3_client._exists(s3_path)
                logger.debug(
                    "Checked file existence in S3",
                    extra={"bucket": bucket, "key": key, "exists": exists},
                )
                return bool(exists)
            if path and self._is_s3_path(path):
                exists = await self.s3_client._exists(path)
                logger.debug(
                    "Checked file existence in S3 via path",
                    extra={"path": path, "exists": exists},
                )
                return bool(exists)
            if path and self._is_abfs_path(path):
                exists = await self.azure_client._exists(path)
                logger.debug(
                    "Checked file existence in ABFS via path",
                    extra={"path": path, "exists": exists},
                )
                return bool(exists)
            if path:
                exists = self.local_client.exists(path)
                logger.debug(
                    "Checked file existence in local filesystem",
                    extra={"path": path, "exists": exists},
                )
                return bool(exists)
            raise ValueError("Either path or bucket and key must be provided")
        except (OSError, PermissionError, TimeoutError, FSTimeoutError) as exception:
            logger.exception(
                "Failed to check file existence",
                extra={"bucket": bucket, "key": key, "path": path},
            )
            raise CheckFileExistenceError(
                f"Failed to check file existence (bucket={bucket}, key={key}, "
                f"path={path})"
            ) from exception

    async def download_file(
        self,
        dest_path: str,
        bucket: str | None = None,
        key: str | None = None,
        source_path: str | None = None,
        max_tries: int = 3,
    ) -> None:
        """Download a file from storage to local filesystem.

        Args:
            dest_path: The destination local path where the file should be saved
            bucket: The bucket name (for S3)
            key: The object key to download (for S3)
            source_path: The source path. Uses S3 if starts with s3://, otherwise local
            max_tries: Number of retry attempts on failure

        Raises:
            DownloadFileError: If download fails after all retries.

        """
        transfer_path: TransferPath = self._resolve_source_backend_and_path(
            bucket=bucket, key=key, path=source_path
        )
        backend_name: str = transfer_path.backend
        src_path: str = transfer_path.path
        for attempt in range(max_tries):
            try:
                logger.debug(
                    "Downloading file",
                    extra={
                        "backend": backend_name,
                        "source": src_path,
                        "dest_path": dest_path,
                    },
                )
                fs = self._get_fs_for_backend(backend_name)
                await _get_file(fs, src_path, dest_path)
                logger.debug(
                    "Downloaded file",
                    extra={
                        "backend": backend_name,
                        "source": src_path,
                        "dest_path": dest_path,
                    },
                )
                return
            except OSError as exception:
                if attempt == max_tries - 1:
                    logger.exception(
                        "Failed to download file after %d retries",
                        max_tries,
                        extra={
                            "bucket": bucket,
                            "key": key,
                            "source_path": source_path,
                            "dest_path": dest_path,
                        },
                    )
                    raise DownloadFileError(
                        f"Failed to download file after {max_tries} retries "
                        f"(bucket={bucket}, key={key}, source_path={source_path}, "
                        f"dest_path={dest_path})"
                    ) from exception
                logger.warning(
                    "Failed to download file, retrying...",
                    extra={"attempt": attempt + 1},
                )
                await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff

    async def get_file_size(
        self, bucket: str | None = None, key: str | None = None, path: str | None = None
    ) -> int:
        """Get the size of a file without downloading it.

        Args:
            bucket: The bucket name (for S3)
            key: The object key (for S3)
            path: The filesystem path. Uses S3 if starts with s3://, otherwise local

        Returns:
            int: File size in bytes

        Raises:
            GetFileSizeError: If getting file size fails.
            ValueError: If neither path nor bucket+key are provided.

        """
        try:
            if bucket and key:
                s3_path = f"s3://{bucket}/{key}"
                logger.debug(
                    "Getting file size from S3", extra={"bucket": bucket, "key": key}
                )
                info = cast(dict[str, Any], await self.s3_client._info(s3_path))
                size = self._size_from_info(info)
                logger.debug(
                    "Got file size from S3",
                    extra={"bucket": bucket, "key": key, "size": size},
                )
                return size
            if path and self._is_s3_path(path):
                logger.debug("Getting file size from S3 via path", extra={"path": path})
                info = cast(dict[str, Any], await self.s3_client._info(path))
                size = self._size_from_info(info)
                logger.debug(
                    "Got file size from S3 via path",
                    extra={"path": path, "size": size},
                )
                return size
            if path and self._is_abfs_path(path):
                logger.debug(
                    "Getting file size from ABFS via path", extra={"path": path}
                )
                info = await self.azure_client._info(path)
                size = self._size_from_info(info)
                logger.debug(
                    "Got file size from ABFS via path",
                    extra={"path": path, "size": size},
                )
                return size
            if path:
                logger.debug(
                    "Getting file size from local filesystem", extra={"path": path}
                )
                info = cast(dict[str, Any], self.local_client.info(path))
                size = self._size_from_info(info)
                logger.debug(
                    "Got file size from local filesystem",
                    extra={"path": path, "size": size},
                )
                return size

            raise ValueError("Either path or bucket and key must be provided")
        except (
            FileNotFoundError,
            PermissionError,
            OSError,
            TimeoutError,
            FSTimeoutError,
        ) as exception:
            logger.exception(
                "Failed to get file size",
                extra={"bucket": bucket, "key": key, "path": path},
            )
            raise GetFileSizeError(
                f"Failed to get file size (bucket={bucket}, key={key}, path={path})"
            ) from exception

    async def get_file_info(
        self, bucket: str | None = None, key: str | None = None, path: str | None = None
    ) -> dict[str, Any]:
        """Get metadata information about a file.

        Args:
            bucket: The bucket name (for S3)
            key: The object key (for S3)
            path: The filesystem path. Uses S3 if starts with s3://, otherwise local

        Returns:
            dict[str, Any]: Dictionary containing file metadata (size, modified
                time, etc.)

        Raises:
            GetFileInfoError: If getting file info fails.
            ValueError: If neither path nor bucket+key are provided.

        """
        try:
            if bucket and key:
                s3_path = f"{S3_PATH_PREFIX}{bucket}/{key}"
                logger.debug(
                    "Getting file info from S3", extra={"bucket": bucket, "key": key}
                )
                info = cast(dict[str, Any], await self.s3_client._info(s3_path))
                logger.debug(
                    "Got file info from S3",
                    extra={"bucket": bucket, "key": key},
                )
                return info
            if path and self._is_s3_path(path):
                logger.debug("Getting file info from S3 via path", extra={"path": path})
                info = cast(dict[str, Any], await self.s3_client._info(path))
                logger.debug(
                    "Got file info from S3 via path",
                    extra={"path": path},
                )
                return info
            if path and self._is_abfs_path(path):
                logger.debug(
                    "Getting file info from ABFS via path", extra={"path": path}
                )
                info = cast(dict[str, Any], await self.azure_client._info(path))
                logger.debug(
                    "Got file info from ABFS via path",
                    extra={"path": path},
                )
                return info
            if path:
                logger.debug(
                    "Getting file info from local filesystem", extra={"path": path}
                )
                info = cast(dict[str, Any], self.local_client.info(path))
                logger.debug(
                    "Got file info from local filesystem",
                    extra={"path": path},
                )
                return info

            raise ValueError("Either path or bucket and key must be provided")
        except (
            FileNotFoundError,
            PermissionError,
            OSError,
            TimeoutError,
            FSTimeoutError,
        ) as exception:
            logger.exception(
                "Failed to get file info",
                extra={"bucket": bucket, "key": key, "path": path},
            )
            raise GetFileInfoError(
                f"Failed to get file info (bucket={bucket}, key={key}, path={path})"
            ) from exception

    async def copy_file(
        self,
        source_bucket: str | None = None,
        source_key: str | None = None,
        source_path: str | None = None,
        dest_bucket: str | None = None,
        dest_key: str | None = None,
        dest_path: str | None = None,
    ) -> None:
        """Copy a file from one location to another.

        Args:
            source_bucket: Source bucket name (for S3 source)
            source_key: Source object key (for S3 source)
            source_path: Source path. Uses S3 if starts with s3://, otherwise local
            dest_bucket: Destination bucket name (for S3 destination)
            dest_key: Destination object key (for S3 destination)
            dest_path: Destination path. Uses S3 if starts with s3://, otherwise local

        Raises:
            CopyFileError: If copying the file fails.

        """
        try:
            src, dst, backend = self._resolve_transfer_paths(
                source_bucket=source_bucket,
                source_key=source_key,
                source_path=source_path,
                dest_bucket=dest_bucket,
                dest_key=dest_key,
                dest_path=dest_path,
            )

            logger.debug("Copying file", extra={"source": src, "destination": dst})
            await self._copy_with_backend(backend=backend, src=src, dst=dst)

            logger.debug("Copied file", extra={"source": src, "destination": dst})
        except (
            FileNotFoundError,
            PermissionError,
            OSError,
            TimeoutError,
            FSTimeoutError,
        ) as exception:
            logger.exception(
                "Failed to copy file",
                extra={"source": src, "destination": dst},
            )
            raise CopyFileError(
                f"Failed to copy file (source={src}, destination={dst})"
            ) from exception

    async def move_file(
        self,
        source_bucket: str | None = None,
        source_key: str | None = None,
        source_path: str | None = None,
        dest_bucket: str | None = None,
        dest_key: str | None = None,
        dest_path: str | None = None,
    ) -> None:
        """Move/rename a file from one location to another.

        Args:
            source_bucket: Source bucket name (for S3 source)
            source_key: Source object key (for S3 source)
            source_path: Source path. Uses S3 if starts with s3://, otherwise local
            dest_bucket: Destination bucket name (for S3 destination)
            dest_key: Destination object key (for S3 destination)
            dest_path: Destination path. Uses S3 if starts with s3://, otherwise local

        Raises:
            MoveFileError: If moving the file fails.

        """
        try:
            src, dst, backend = self._resolve_transfer_paths(
                source_bucket=source_bucket,
                source_key=source_key,
                source_path=source_path,
                dest_bucket=dest_bucket,
                dest_key=dest_key,
                dest_path=dest_path,
            )

            logger.debug("Moving file", extra={"source": src, "destination": dst})
            await self._move_with_backend(backend=backend, src=src, dst=dst)

            logger.debug("Moved file", extra={"source": src, "destination": dst})
        except (
            FileNotFoundError,
            PermissionError,
            OSError,
            TimeoutError,
            FSTimeoutError,
        ) as exception:
            logger.exception(
                "Failed to move file",
                extra={"source": src, "destination": dst},
            )
            raise MoveFileError(
                f"Failed to move file (source={src}, destination={dst})"
            ) from exception
