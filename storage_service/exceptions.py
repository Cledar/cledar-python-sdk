class ObjectStorageError(Exception):
    pass


class RequiredBucketNotFoundError(ObjectStorageError):
    pass


class UploadBufferError(ObjectStorageError):
    pass


class UploadFileError(ObjectStorageError):
    pass


class ReadFileError(ObjectStorageError):
    pass


class DownloadFileError(ObjectStorageError):
    pass


class ListObjectsError(ObjectStorageError):
    pass


class DeleteFileError(ObjectStorageError):
    pass


class GetFileSizeError(ObjectStorageError):
    pass


class GetFileInfoError(ObjectStorageError):
    pass


class CopyFileError(ObjectStorageError):
    pass


class MoveFileError(ObjectStorageError):
    pass


class CheckFileExistenceError(ObjectStorageError):
    pass
