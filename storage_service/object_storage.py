import io
import logging
from dataclasses import dataclass
from typing import Any, cast

import fsspec
from fsspec.exceptions import FSTimeoutError  # type: ignore[import-untyped]

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

logger = logging.getLogger("object_storage_service")


@dataclass
class ObjectStorageServiceConfig:
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_max_concurrency: int


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

    @staticmethod
    def _is_s3_path(path: str | None) -> bool:
        if path is None:
            return False
        return path.lower().startswith("s3://")

    def is_alive(self) -> bool:
        try:
            self.client.ls(path="")
            return True
        except (OSError, PermissionError, TimeoutError, FSTimeoutError):
            return False

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
                buffer.seek(0)
                with self.client.open(path=f"s3://{bucket}/{key}", mode="wb") as fobj:
                    fobj.write(buffer.getbuffer())
                logger.debug(
                    "Uploaded file from buffer", extra={"bucket": bucket, "key": key}
                )
            elif self._is_s3_path(destination_path):
                logger.debug(
                    "Uploading file from buffer to S3 via path",
                    extra={"destination_path": destination_path},
                )
                buffer.seek(0)
                with self.client.open(path=destination_path, mode="wb") as fobj:
                    fobj.write(buffer.getbuffer())
            elif destination_path:
                logger.debug(
                    "Uploading file from buffer to local filesystem",
                    extra={"destination_path": destination_path},
                )
                buffer.seek(0)
                with self.local_client.open(path=destination_path, mode="wb") as fobj:
                    fobj.write(buffer.getbuffer())
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
        for attempt in range(max_tries):
            try:
                if bucket and key:
                    logger.debug(
                        "Reading file from S3", extra={"bucket": bucket, "key": key}
                    )
                    with self.client.open(
                        path=f"s3://{bucket}/{key}", mode="rb"
                    ) as fobj:
                        content: bytes = fobj.read()
                    logger.debug(
                        "File read from S3", extra={"bucket": bucket, "key": key}
                    )
                    return content
                if self._is_s3_path(path):
                    logger.debug("Reading file from S3 via path", extra={"path": path})
                    with self.client.open(path=path, mode="rb") as fobj:
                        content: bytes = fobj.read()  # type: ignore[no-redef]
                    logger.debug("File read from S3 via path", extra={"path": path})
                    return content
                if path:
                    logger.debug(
                        "Reading file from local filesystem", extra={"path": path}
                    )
                    with self.local_client.open(path=path, mode="rb") as fobj:
                        content: bytes = fobj.read()  # type: ignore[no-redef]
                    logger.debug(
                        "File read from local filesystem", extra={"path": path}
                    )
                    return content
                raise ValueError("Either path or bucket and key must be provided")
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
                    extra={"bucket": bucket, "key": key},
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
            if bucket and key:
                logger.debug(
                    "Uploading file from filesystem to S3",
                    extra={"bucket": bucket, "key": key},
                )
                self.client.put(lpath=file_path, rpath=f"s3://{bucket}/{key}")
                logger.debug(
                    "Uploaded file from filesystem to S3",
                    extra={"bucket": bucket, "key": key},
                )
            elif self._is_s3_path(destination_path):
                logger.debug(
                    "Uploading file from filesystem to S3 via path",
                    extra={"destination_path": destination_path},
                )
                self.client.put(lpath=file_path, rpath=destination_path)
            elif destination_path:
                logger.debug(
                    "Uploading file from filesystem to local filesystem",
                    extra={"destination_path": destination_path},
                )
                self.local_client.put(lpath=file_path, rpath=destination_path)
                logger.debug(
                    "Uploaded file from filesystem to local filesystem",
                    extra={"destination_path": destination_path},
                )
            else:
                raise ValueError(
                    "Either destination_path or bucket and key must be provided"
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
            if self._is_s3_path(path):
                s3_path = path
                logger.debug(
                    "Listing objects from S3 via path",
                    extra={"path": s3_path, "recursive": recursive},
                )
                if recursive:
                    objects = self.client.find(s3_path)
                else:
                    objects = self.client.ls(s3_path, detail=False)
                logger.debug(
                    "Listed objects from S3 via path",
                    extra={"path": s3_path, "count": len(objects)},
                )
                return cast(list[str], objects)
            if path:
                logger.debug(
                    "Listing objects from local filesystem",
                    extra={"path": path, "recursive": recursive},
                )
                if recursive:
                    objects = self.local_client.find(path)
                else:
                    objects = self.local_client.ls(path, detail=False)

                logger.debug(
                    "Listed objects from local filesystem",
                    extra={"path": path, "count": len(objects)},
                )
                return cast(list[str], objects)
            if bucket:
                s3_path = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"
                logger.debug(
                    "Listing objects from S3",
                    extra={"bucket": bucket, "prefix": prefix, "recursive": recursive},
                )

                if recursive:
                    objects = self.client.find(s3_path)
                else:
                    objects = self.client.ls(s3_path, detail=False)

                # fsspec can return paths with or without s3:// prefix
                # depending on implementation
                keys = []
                for obj in objects:
                    if obj.startswith(f"s3://{bucket}/"):
                        keys.append(obj.replace(f"s3://{bucket}/", ""))
                    elif obj.startswith(f"{bucket}/"):
                        keys.append(obj.replace(f"{bucket}/", ""))
                    else:
                        # object might be just the key without bucket prefix
                        keys.append(obj)

                logger.debug(
                    "Listed objects from S3",
                    extra={
                        "bucket": bucket,
                        "prefix": prefix,
                        "count": len(keys),
                    },
                )
                return cast(list[str], keys)
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
                s3_path = f"s3://{bucket}/{key}"
                logger.debug(
                    "Deleting file from S3", extra={"bucket": bucket, "key": key}
                )
                self.client.rm(s3_path)
                logger.debug(
                    "Deleted file from S3", extra={"bucket": bucket, "key": key}
                )
            elif self._is_s3_path(path):
                logger.debug("Deleting file from S3 via path", extra={"path": path})
                self.client.rm(path)
                logger.debug("Deleted file from S3 via path", extra={"path": path})
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
                s3_path = f"s3://{bucket}/{key}"
                exists = self.client.exists(s3_path)
                logger.debug(
                    "Checked file existence in S3",
                    extra={"bucket": bucket, "key": key, "exists": exists},
                )
                return bool(exists)
            if self._is_s3_path(path):
                exists = self.client.exists(path)
                logger.debug(
                    "Checked file existence in S3 via path",
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
        for attempt in range(max_tries):
            try:
                if bucket and key:
                    s3_path = f"s3://{bucket}/{key}"
                    logger.debug(
                        "Downloading file from S3",
                        extra={"bucket": bucket, "key": key, "dest_path": dest_path},
                    )
                    self.client.get(s3_path, dest_path)
                    logger.debug(
                        "Downloaded file from S3",
                        extra={"bucket": bucket, "key": key, "dest_path": dest_path},
                    )
                    return
                if self._is_s3_path(source_path):
                    logger.debug(
                        "Copying file from S3 via path",
                        extra={"source_path": source_path, "dest_path": dest_path},
                    )
                    self.client.get(source_path, dest_path)
                    logger.debug(
                        "Copied file from S3 via path",
                        extra={"source_path": source_path, "dest_path": dest_path},
                    )
                    return
                if source_path:
                    logger.debug(
                        "Copying file from local filesystem",
                        extra={"source_path": source_path, "dest_path": dest_path},
                    )
                    self.local_client.get(source_path, dest_path)
                    logger.debug(
                        "Copied file from local filesystem",
                        extra={"source_path": source_path, "dest_path": dest_path},
                    )
                    return
                raise ValueError(
                    "Either source_path or bucket and key must be provided"
                )
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
                info = self.client.info(s3_path)
                size = info.get("size", 0)
                logger.debug(
                    "Got file size from S3",
                    extra={"bucket": bucket, "key": key, "size": size},
                )
                return int(size)
            if self._is_s3_path(path):
                logger.debug("Getting file size from S3 via path", extra={"path": path})
                info = self.client.info(path)
                size = info.get("size", 0)
                logger.debug(
                    "Got file size from S3 via path",
                    extra={"path": path, "size": size},
                )
                return int(size)
            if path:
                logger.debug(
                    "Getting file size from local filesystem", extra={"path": path}
                )
                info = self.local_client.info(path)
                size = info.get("size", 0)
                logger.debug(
                    "Got file size from local filesystem",
                    extra={"path": path, "size": size},
                )
                return int(size)

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
                s3_path = f"s3://{bucket}/{key}"
                logger.debug(
                    "Getting file info from S3", extra={"bucket": bucket, "key": key}
                )
                info = self.client.info(s3_path)
                logger.debug(
                    "Got file info from S3",
                    extra={"bucket": bucket, "key": key},
                )
                return cast(dict[str, Any], info)
            if self._is_s3_path(path):
                logger.debug("Getting file info from S3 via path", extra={"path": path})
                info = self.client.info(path)
                logger.debug(
                    "Got file info from S3 via path",
                    extra={"path": path},
                )
                return cast(dict[str, Any], info)
            if path:
                logger.debug(
                    "Getting file info from local filesystem", extra={"path": path}
                )
                info = self.local_client.info(path)
                logger.debug(
                    "Got file info from local filesystem",
                    extra={"path": path},
                )
                return cast(dict[str, Any], info)

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
    ) -> tuple[str, str, bool]:
        """
        Resolve source and destination paths for copy/move operations and
        return whether either side is S3-backed.
        """
        src_is_s3 = False
        if source_bucket and source_key:
            src: str = f"s3://{source_bucket}/{source_key}"
            src_is_s3 = True
        elif self._is_s3_path(source_path):
            src = cast(str, source_path)
            src_is_s3 = True
        elif source_path:
            src = source_path
        else:
            raise ValueError(
                "Either source_path or source_bucket and source_key must be provided"
            )

        dst_is_s3 = False
        if dest_bucket and dest_key:
            dst: str = f"s3://{dest_bucket}/{dest_key}"
            dst_is_s3 = True
        elif self._is_s3_path(dest_path):
            dst = cast(str, dest_path)
            dst_is_s3 = True
        elif dest_path:
            dst = dest_path
        else:
            raise ValueError(
                "Either dest_path or dest_bucket and dest_key must be provided"
            )

        return src, dst, bool(src_is_s3 or dst_is_s3)

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
            src, dst, s3_involved = self._resolve_transfer_paths(
                source_bucket=source_bucket,
                source_key=source_key,
                source_path=source_path,
                dest_bucket=dest_bucket,
                dest_key=dest_key,
                dest_path=dest_path,
            )

            logger.debug("Copying file", extra={"source": src, "destination": dst})

            if s3_involved:
                self.client.copy(src, dst)
            else:
                self.local_client.copy(src, dst)

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
            src, dst, s3_involved = self._resolve_transfer_paths(
                source_bucket=source_bucket,
                source_key=source_key,
                source_path=source_path,
                dest_bucket=dest_bucket,
                dest_key=dest_key,
                dest_path=dest_path,
            )

            logger.debug("Moving file", extra={"source": src, "destination": dst})

            if s3_involved:
                self.client.move(src, dst)
            else:
                self.local_client.move(src, dst)

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
