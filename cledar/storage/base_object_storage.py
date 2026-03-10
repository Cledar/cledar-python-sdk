"""Base class for object storage services with common functionality."""

from typing import Any, cast

from .constants import (
    ABFS_PATH_PREFIX,
    ABFSS_PATH_PREFIX,
    S3_PATH_PREFIX,
)
from .models import ObjectStorageServiceConfig, TransferPath


class BaseObjectStorageService:
    """Base class containing common functionality for storage services."""

    s3_client: Any = None
    local_client: Any = None
    azure_client: Any = None
    config: ObjectStorageServiceConfig

    @staticmethod
    def _is_s3_path(path: str | None) -> bool:
        """Check if a path is an S3 path.

        Args:
            path: Path to check.

        Returns:
            bool: True if the path starts with s3:// prefix.

        """
        if path is None:
            return False
        return path.lower().startswith(S3_PATH_PREFIX)

    @staticmethod
    def _is_abfs_path(path: str | None) -> bool:
        """Check if a path is an Azure Blob Storage path.

        Args:
            path: Path to check.

        Returns:
            bool: True if the path starts with abfs:// or abfss:// prefix.

        """
        if path is None:
            return False
        lower = path.lower()
        return lower.startswith((ABFS_PATH_PREFIX, ABFSS_PATH_PREFIX))

    def _normalize_s3_keys(self, bucket: str, objects: list[str]) -> list[str]:
        """Normalize S3 object paths to keys by removing bucket prefix.

        Args:
            bucket: S3 bucket name.
            objects: List of full S3 paths.

        Returns:
            list[str]: List of normalized keys without bucket prefix.

        """
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
        """Extract file size from file info dictionary.

        Args:
            info: File info dictionary.

        Returns:
            int: File size in bytes.

        """
        return int(info.get("size", 0))

    def _get_fs_for_backend(self, backend: str) -> Any:
        """Get filesystem client for the specified backend.

        Args:
            backend: Backend type (s3, abfs, or local).

        Returns:
            Any: Filesystem client for the backend.

        """
        if backend == "s3":
            return self.s3_client
        if backend == "abfs":
            return self.azure_client
        return self.local_client

    def _resolve_source_backend_and_path(
        self, bucket: str | None, key: str | None, path: str | None
    ) -> TransferPath:
        """Resolve source backend and path from various input formats.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            path: Full path (can be S3, ABFS, or local).

        Returns:
            TransferPath: Transfer path with backend type and resolved path.

        Raises:
            ValueError: If neither path nor bucket+key are provided.

        """
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
        """Resolve destination backend and path from various input formats.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            destination_path: Full destination path (can be S3, ABFS, or local).

        Returns:
            TransferPath: Transfer path with backend type and resolved path.

        Raises:
            ValueError: If neither destination_path nor bucket+key are provided.

        """
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
        """Resolve backend type from path.

        Args:
            path: Full path (can be S3, ABFS, or local).

        Returns:
            Transfer path with backend type and path.

        Raises:
            ValueError: If path is not provided.

        """
        if path and self._is_s3_path(path):
            return TransferPath(backend="s3", path=path)
        if path and self._is_abfs_path(path):
            return TransferPath(backend="abfs", path=path)
        if path:
            return TransferPath(backend="local", path=path)
        raise ValueError("Path must be provided")

    def _resolve_transfer_paths(
        self,
        source_bucket: str | None,
        source_key: str | None,
        source_path: str | None,
        dest_bucket: str | None,
        dest_key: str | None,
        dest_path: str | None,
    ) -> tuple[str, str, str]:
        """Resolve source and destination paths for copy/move operations.

        Args:
            source_bucket: Source S3 bucket name.
            source_key: Source S3 object key.
            source_path: Full source path (can be S3, ABFS, or local).
            dest_bucket: Destination S3 bucket name.
            dest_key: Destination S3 object key.
            dest_path: Full destination path (can be S3, ABFS, or local).

        Returns:
            tuple[str, str, str]: Tuple of (source_path, destination_path,
                backend_type).

        Raises:
            ValueError: If source or destination parameters are missing.

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
