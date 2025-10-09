from enum import Enum


class UploadStatus(Enum):
    CHUNK_RECEIVED = "chunk_received"
    COMPLETE = "complete"
    ERROR = "error"
    BAD_REQUEST = "bad_request"
