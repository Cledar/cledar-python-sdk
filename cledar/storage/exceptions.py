"""Exceptions for object storage operations."""


class ObjectStorageError(Exception):
    """Base exception for object storage errors."""

    pass


class RequiredBucketNotFoundError(ObjectStorageError):
    """Exception raised when a required bucket is not found."""

    pass


class UploadBufferError(ObjectStorageError):
    """Exception raised when uploading a buffer fails."""

    pass


class UploadFileError(ObjectStorageError):
    """Exception raised when uploading a file fails."""

    pass


class ReadFileError(ObjectStorageError):
    """Exception raised when reading a file fails."""

    pass


class DownloadFileError(ObjectStorageError):
    """Exception raised when downloading a file fails."""

    pass


class ListObjectsError(ObjectStorageError):
    """Exception raised when listing objects fails."""

    pass


class DeleteFileError(ObjectStorageError):
    """Exception raised when deleting a file fails."""

    pass


class GetFileSizeError(ObjectStorageError):
    """Exception raised when getting file size fails."""

    pass


class GetFileInfoError(ObjectStorageError):
    """Exception raised when getting file info fails."""

    pass


class CopyFileError(ObjectStorageError):
    """Exception raised when copying a file fails."""

    pass


class MoveFileError(ObjectStorageError):
    """Exception raised when moving a file fails."""

    pass


class CheckFileExistenceError(ObjectStorageError):
    """Exception raised when checking file existence fails."""

    pass
