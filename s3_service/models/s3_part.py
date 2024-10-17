from typing import TypedDict
from dataclasses import dataclass


@dataclass(frozen=True)
class S3Part(TypedDict):
    ETag: str
    PartNumber: int
