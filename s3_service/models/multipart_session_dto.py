from dataclasses import dataclass

from .s3_part import S3Part  # pylint: disable=relative-beyond-top-level


@dataclass
class MultipartSessionDto:
    gen_filename: str
    file_name: str
    upload_id: str
    bucket: str
    parts: list[S3Part]
    last_chunk_no: int = 0
