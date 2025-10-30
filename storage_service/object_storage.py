import io
import logging
from typing import Any, cast

import fsspec
from fsspec.exceptions import FSTimeoutError

from .constants import (
    ABFS_PATH_PREFIX,
    ABFSS_PATH_PREFIX,
    S3_PATH_PREFIX,
)
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

logger = logging.getLogger("object_storage_service")


class ObjectStorageService:
    client: Any = None

    def __init__(self, config: ObjectStorageServiceConfig) -> None:
        self.client = fsspec.filesystem(
            "s3",
            key=config.s3_access_key,
            secret=config.s3_secret_key,
            client_kwargs={"endpoint_url": config.s3_endpoint_url},
            max_concurrency=config.s3_max_concurrency,
        )
        logger.info(
            "Initiated filesystem", extra={"endpoint_url": config.s3_endpoint_url}
        )
        self.local_client = fsspec.filesystem("file")

        if config.azure_account_name and config.azure_account_key:
            self.azure_client = fsspec.filesystem(
                "abfs",
                account_name=config.azure_account_name,
                account_key=config.azure_account_key,
            )
        else:
            self.azure_client = None

    @staticmethod
    def _is_s3_path(path: str | None) -> bool:
        if path is None:
            return False
        return path.lower().startswith(S3_PATH_PREFIX)

    @staticmethod
    def _is_abfs_path(path: str | None) -> bool:
        if path is None:
            return False
        lower = path.lower()
        return lower.startswith((ABFS_PATH_PREFIX, ABFSS_PATH_PREFIX))

    def is_alive(self) -> bool:
        try:
            self.client.ls(path="")
            return True
        except (OSError, PermissionError, TimeoutError, FSTimeoutError):
            return False

    def _write_buffer_to_s3_key(
        self, buffer: io.BytesIO, bucket: str, key: str
    ) -> None:
        buffer.seek(0)
        with self.client.open(
            path=f"{S3_PATH_PREFIX}{bucket}/{key}", mode="wb"
        ) as fobj:
            fobj.write(buffer.getbuffer())

    def _write_buffer_to_s3_path(
        self, buffer: io.BytesIO, destination_path: str
    ) -> None:
        buffer.seek(0)
        with self.client.open(path=destination_path, mode="wb") as fobj:
            fobj.write(buffer.getbuffer())

    def _write_buffer_to_abfs_path(
        self, buffer: io.BytesIO, destination_path: str
    ) -> None:
        buffer.seek(0)
        with self.azure_client.open(path=destination_path, mode="wb") as fobj:
            fobj.write(buffer.getbuffer())

    def _write_buffer_to_local_path(
        self, buffer: io.BytesIO, destination_path: str
    ) -> None:
        buffer.seek(0)
        with self.local_client.open(path=destination_path, mode="wb") as fobj:
            fobj.write(buffer.getbuffer())

    def _read_from_s3_key(self, bucket: str, key: str) -> bytes:
        with self.client.open(
            path=f"{S3_PATH_PREFIX}{bucket}/{key}", mode="rb"
        ) as fobj:
            data: bytes = fobj.read()
            return data

    def _read_from_s3_path(self, path: str) -> bytes:
        with self.client.open(path=path, mode="rb") as fobj:
            data: bytes = fobj.read()
            return data

    def _read_from_abfs_path(self, path: str) -> bytes:
        with self.azure_client.open(path=path, mode="rb") as fobj:
            data: bytes = fobj.read()
            return data

    def _read_from_local_path(self, path: str) -> bytes:
        with self.local_client.open(path=path, mode="rb") as fobj:
            data: bytes = fobj.read()
            return data

    def _put_file(self, fs: Any, lpath: str, rpath: str) -> None:
        fs.put(lpath=lpath, rpath=rpath)

    def _get_file(self, fs: Any, src: str, dst: str) -> None:
        fs.get(src, dst)

    def _list_via_find_or_ls(self, fs: Any, path: str, recursive: bool) -> list[str]:
        if recursive:
            return cast(list[str], fs.find(path))
        return cast(list[str], fs.ls(path, detail=False))

    def _normalize_s3_keys(self, bucket: str, objects: list[str]) -> list[str]:
        keys: list[str] = []
        for obj in objects:
            if obj.startswith(f"{S3_PATH_PREFIX}{bucket}/"):
                keys.append(obj.replace(f"{S3_PATH_PREFIX}{bucket}/", ""))
            elif obj.startswith(f"{bucket}/"):
                keys.append(obj.replace(f"{bucket}/", ""))
            else:
                keys.append(obj)
        return keys

    def _size_from_info(self, info: dict[str, Any]) -> int:
        return int(info.get("size", 0))

    def _copy_with_backend(self, backend: str, src: str, dst: str) -> None:
        if backend == "s3":
            self.client.copy(src, dst)
            return
        if backend == "abfs":
            self.azure_client.copy(src, dst)
            return
        if backend == "local":
            self.local_client.copy(src, dst)
            return
        with (
            fsspec.open(src, mode="rb") as src_f,
            fsspec.open(dst, mode="wb") as dst_f,
        ):
            dst_f.write(src_f.read())

    def _move_with_backend(self, backend: str, src: str, dst: str) -> None:
        if backend == "s3":
            self.client.move(src, dst)
            return
        if backend == "abfs":
            self.azure_client.move(src, dst)
            return
        if backend == "local":
            self.local_client.move(src, dst)
            return
        with (
            fsspec.open(src, mode="rb") as src_f,
            fsspec.open(dst, mode="wb") as dst_f,
        ):
            dst_f.write(src_f.read())
        if self._is_s3_path(src):
            self.client.rm(src)
        elif self._is_abfs_path(src):
            self.azure_client.rm(src)
        else:
            self.local_client.rm(src)

    def _get_fs_for_backend(self, backend: str) -> Any:
        if backend == "s3":
            return self.client
        if backend == "abfs":
            return self.azure_client
        return self.local_client

    def _resolve_source_backend_and_path(
        self, bucket: str | None, key: str | None, path: str | None
    ) -> TransferPath:
        if bucket and key:
            return TransferPath(backend="s3", path=f"{S3_PATH_PREFIX}{bucket}/{key}")
        if path and self._is_s3_path(path):
            return TransferPath(backend="s3", path=path)
        if path and self._is_abfs_path(path):
            return TransferPath(backend="abfs", path=path)
        if path:
            return TransferPath(backend="local", path=path)
        raise ValueError("Either path or bucket and key must be provided")

    def _resolve_dest_backend_and_path(
        self, bucket: str | None, key: str | None, destination_path: str | None
    ) -> TransferPath:
        if bucket and key:
            return TransferPath(backend="s3", path=f"{S3_PATH_PREFIX}{bucket}/{key}")
        if destination_path and self._is_s3_path(destination_path):
            return TransferPath(backend="s3", path=destination_path)
        if destination_path and self._is_abfs_path(destination_path):
            return TransferPath(backend="abfs", path=destination_path)
        if destination_path:
            return TransferPath(backend="local", path=destination_path)
        raise ValueError("Either destination_path or bucket and key must be provided")

    def _resolve_path_backend(self, path: str | None) -> TransferPath:
        if path and self._is_s3_path(path):
            return TransferPath(backend="s3", path=path)
        if path and self._is_abfs_path(path):
            return TransferPath(backend="abfs", path=path)
        if path:
            return TransferPath(backend="local", path=path)
        raise ValueError("Path must be provided")

    def _read_from_backend_path(self, backend: str, src_path: str) -> bytes:
        if backend == "s3":
            return self._read_from_s3_path(src_path)
        if backend == "abfs":
            return self._read_from_abfs_path(src_path)
        return self._read_from_local_path(src_path)

    def has_bucket(self, bucket: str, throw: bool = False) -> bool:
        try:
            self.client.ls(path=bucket)
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

    def upload_buffer(
        self,
        buffer: io.BytesIO,
        bucket: str | None = None,
        key: str | None = None,
        destination_path: str | None = None,
    ) -> None:
        try:
            if bucket and key:
                self._write_buffer_to_s3_key(buffer=buffer, bucket=bucket, key=key)
                logger.debug(
                    "Uploaded file from buffer", extra={"bucket": bucket, "key": key}
                )
            elif destination_path and self._is_s3_path(destination_path):
                logger.debug(
                    "Uploading file from buffer to S3 via path",
                    extra={"destination_path": destination_path},
                )
                self._write_buffer_to_s3_path(
                    buffer=buffer, destination_path=destination_path
                )
            elif destination_path and self._is_abfs_path(destination_path):
                logger.debug(
                    "Uploading file from buffer to ABFS via path",
                    extra={"destination_path": destination_path},
                )
                self._write_buffer_to_abfs_path(
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

    def read_file(
        self,
        bucket: str | None = None,
        key: str | None = None,
        path: str | None = None,
        max_tries: int = 3,
    ) -> bytes:
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
                content = self._read_from_backend_path(backend_name, src_path)
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
        raise NotImplementedError("This should never be reached")

    def upload_file(
        self,
        file_path: str,
        bucket: str | None = None,
        key: str | None = None,
        destination_path: str | None = None,
    ) -> None:
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
            self._put_file(fs, lpath=file_path, rpath=dst_path)
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

    def list_objects(
        self,
        bucket: str | None = None,
        prefix: str = "",
        path: str | None = None,
        recursive: bool = True,
    ) -> list[str]:
        """
        List objects in storage with optional prefix filtering.

        Args:
            bucket: The bucket name (for S3)
            prefix: Optional prefix to filter objects (for S3)
            path: The filesystem path. Uses S3 if starts with s3://, otherwise local
            recursive: If True, list all objects recursively

        Returns:
            List of object keys/paths
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
                objects = self._list_via_find_or_ls(fs, resolved_path, recursive)
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
                objects = self._list_via_find_or_ls(self.client, s3_path, recursive)
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

    def delete_file(
        self, bucket: str | None = None, key: str | None = None, path: str | None = None
    ) -> None:
        """
        Delete a single object from storage.

        Args:
            bucket: The bucket name (for S3)
            key: The object key to delete (for S3)
            path: The filesystem path. Uses S3 if starts with s3://, otherwise local
        """
        try:
            if bucket and key:
                s3_path = f"{S3_PATH_PREFIX}{bucket}/{key}"
                logger.debug(
                    "Deleting file from S3", extra={"bucket": bucket, "key": key}
                )
                self.client.rm(s3_path)
                logger.debug(
                    "Deleted file from S3", extra={"bucket": bucket, "key": key}
                )
            elif path and self._is_s3_path(path):
                logger.debug("Deleting file from S3 via path", extra={"path": path})
                self.client.rm(path)
                logger.debug("Deleted file from S3 via path", extra={"path": path})
            elif path and self._is_abfs_path(path):
                logger.debug("Deleting file from ABFS via path", extra={"path": path})
                self.azure_client.rm(path)
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

    def file_exists(
        self, bucket: str | None = None, key: str | None = None, path: str | None = None
    ) -> bool:
        """
        Check if a specific file exists in storage.

        Args:
            bucket: The bucket name (for S3)
            key: The object key to check (for S3)
            path: The filesystem path. Uses S3 if starts with s3://, otherwise local

        Returns:
            True if the file exists, False otherwise
        """
        try:
            if bucket and key:
                s3_path = f"{S3_PATH_PREFIX}{bucket}/{key}"
                exists = self.client.exists(s3_path)
                logger.debug(
                    "Checked file existence in S3",
                    extra={"bucket": bucket, "key": key, "exists": exists},
                )
                return bool(exists)
            if path and self._is_s3_path(path):
                exists = self.client.exists(path)
                logger.debug(
                    "Checked file existence in S3 via path",
                    extra={"path": path, "exists": exists},
                )
                return bool(exists)
            if path and self._is_abfs_path(path):
                exists = self.azure_client.exists(path)
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

    def download_file(
        self,
        dest_path: str,
        bucket: str | None = None,
        key: str | None = None,
        source_path: str | None = None,
        max_tries: int = 3,
    ) -> None:
        """
        Download a file from storage to local filesystem.

        Args:
            dest_path: The destination local path where the file should be saved
            bucket: The bucket name (for S3)
            key: The object key to download (for S3)
            source_path: The source path. Uses S3 if starts with s3://, otherwise local
            max_tries: Number of retry attempts on failure
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
                self._get_file(fs, src_path, dest_path)
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

    def get_file_size(
        self, bucket: str | None = None, key: str | None = None, path: str | None = None
    ) -> int:
        """
        Get the size of a file without downloading it.

        Args:
            bucket: The bucket name (for S3)
            key: The object key (for S3)
            path: The filesystem path. Uses S3 if starts with s3://, otherwise local

        Returns:
            File size in bytes
        """
        try:
            if bucket and key:
                s3_path = f"s3://{bucket}/{key}"
                logger.debug(
                    "Getting file size from S3", extra={"bucket": bucket, "key": key}
                )
                info = cast(dict[str, Any], self.client.info(s3_path))
                size = self._size_from_info(info)
                logger.debug(
                    "Got file size from S3",
                    extra={"bucket": bucket, "key": key, "size": size},
                )
                return size
            if path and self._is_s3_path(path):
                logger.debug("Getting file size from S3 via path", extra={"path": path})
                info = cast(dict[str, Any], self.client.info(path))
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
                info = self.azure_client.info(path)
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

    def get_file_info(
        self, bucket: str | None = None, key: str | None = None, path: str | None = None
    ) -> dict[str, Any]:
        """
        Get metadata information about a file.

        Args:
            bucket: The bucket name (for S3)
            key: The object key (for S3)
            path: The filesystem path. Uses S3 if starts with s3://, otherwise local

        Returns:
            Dictionary containing file metadata (size, modified time, etc.)
        """
        try:
            if bucket and key:
                s3_path = f"{S3_PATH_PREFIX}{bucket}/{key}"
                logger.debug(
                    "Getting file info from S3", extra={"bucket": bucket, "key": key}
                )
                info = cast(dict[str, Any], self.client.info(s3_path))
                logger.debug(
                    "Got file info from S3",
                    extra={"bucket": bucket, "key": key},
                )
                return info
            if path and self._is_s3_path(path):
                logger.debug("Getting file info from S3 via path", extra={"path": path})
                info = cast(dict[str, Any], self.client.info(path))
                logger.debug(
                    "Got file info from S3 via path",
                    extra={"path": path},
                )
                return info
            if path and self._is_abfs_path(path):
                logger.debug(
                    "Getting file info from ABFS via path", extra={"path": path}
                )
                info = cast(dict[str, Any], self.azure_client.info(path))
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

    def _resolve_transfer_paths(
        self,
        source_bucket: str | None,
        source_key: str | None,
        source_path: str | None,
        dest_bucket: str | None,
        dest_key: str | None,
        dest_path: str | None,
    ) -> tuple[str, str, str]:
        """
        Resolve source and destination paths for copy/move operations and
        identify which backend to use (s3, abfs, local, or mixed).
        """
        src_is_s3 = False
        src_is_abfs = False
        if source_bucket and source_key:
            src: str = f"{S3_PATH_PREFIX}{source_bucket}/{source_key}"
            src_is_s3 = True
        elif self._is_s3_path(source_path):
            src = cast(str, source_path)
            src_is_s3 = True
        elif self._is_abfs_path(source_path):
            src = cast(str, source_path)
            src_is_abfs = True
        elif source_path:
            src = source_path
        else:
            raise ValueError(
                "Either source_path or source_bucket and source_key must be provided"
            )

        dst_is_s3 = False
        dst_is_abfs = False
        if dest_bucket and dest_key:
            dst: str = f"{S3_PATH_PREFIX}{dest_bucket}/{dest_key}"
            dst_is_s3 = True
        elif self._is_s3_path(dest_path):
            dst = cast(str, dest_path)
            dst_is_s3 = True
        elif self._is_abfs_path(dest_path):
            dst = cast(str, dest_path)
            dst_is_abfs = True
        elif dest_path:
            dst = dest_path
        else:
            raise ValueError(
                "Either dest_path or dest_bucket and dest_key must be provided"
            )

        if (src_is_s3 or dst_is_s3) and not (src_is_abfs or dst_is_abfs):
            backend = "s3"
        elif (src_is_abfs or dst_is_abfs) and not (src_is_s3 or dst_is_s3):
            backend = "abfs"
        elif (src_is_s3 or dst_is_s3) and (src_is_abfs or dst_is_abfs):
            backend = "mixed"
        else:
            backend = "local"

        return src, dst, backend

    def copy_file(
        self,
        source_bucket: str | None = None,
        source_key: str | None = None,
        source_path: str | None = None,
        dest_bucket: str | None = None,
        dest_key: str | None = None,
        dest_path: str | None = None,
    ) -> None:
        """
        Copy a file from one location to another.

        Args:
            source_bucket: Source bucket name (for S3 source)
            source_key: Source object key (for S3 source)
            source_path: Source path. Uses S3 if starts with s3://, otherwise local
            dest_bucket: Destination bucket name (for S3 destination)
            dest_key: Destination object key (for S3 destination)
            dest_path: Destination path. Uses S3 if starts with s3://, otherwise local
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
            self._copy_with_backend(backend=backend, src=src, dst=dst)

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

    def move_file(
        self,
        source_bucket: str | None = None,
        source_key: str | None = None,
        source_path: str | None = None,
        dest_bucket: str | None = None,
        dest_key: str | None = None,
        dest_path: str | None = None,
    ) -> None:
        """
        Move/rename a file from one location to another.

        Args:
            source_bucket: Source bucket name (for S3 source)
            source_key: Source object key (for S3 source)
            source_path: Source path. Uses S3 if starts with s3://, otherwise local
            dest_bucket: Destination bucket name (for S3 destination)
            dest_key: Destination object key (for S3 destination)
            dest_path: Destination path. Uses S3 if starts with s3://, otherwise local
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
            self._move_with_backend(backend=backend, src=src, dst=dst)

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
