from dataclasses import dataclass
from typing import Any


@dataclass
class UploadChunkDto:
    bucket: str
    body: Any
    chunk_no: int
    total_chunks: int
    file_name: str
    session_id: str
