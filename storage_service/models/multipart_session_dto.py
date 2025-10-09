from dataclasses import dataclass, field
from threading import RLock

from .object_storage_part import ObjectStoragePart  # pylint: disable=relative-beyond-top-level


@dataclass
# pylint: disable=too-many-instance-attributes
class MultipartSessionDto:
    gen_filename: str
    file_name: str
    bucket: str
    parts: list[ObjectStoragePart] = field(default_factory=list)
    upload_id: str = ""
    total_chunks: int = 0
    uploaded_chunk_numbers: set[int] = field(default_factory=set)
    lock: RLock = field(default_factory=RLock)
