from typing import TypedDict
from dataclasses import dataclass


@dataclass(frozen=True)
class ObjectStoragePart(TypedDict):
    ETag: str
    PartNumber: int
