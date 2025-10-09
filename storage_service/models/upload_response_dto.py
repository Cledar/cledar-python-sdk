from dataclasses import dataclass
from typing import Optional

from .upload_status import UploadStatus  # pylint: disable=relative-beyond-top-level


@dataclass
class UploadResponseDto:
    status: UploadStatus
    message: Optional[str] = None
    gen_filename: Optional[str] = None
    file_name: Optional[str] = None
    chunk_number: Optional[int] = None
    total_chunks: Optional[int] = None
    session_id: Optional[str] = None
