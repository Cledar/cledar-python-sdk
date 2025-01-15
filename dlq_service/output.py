from typing import Any
from pydantic import BaseModel


class FailedMessageData(BaseModel):
    raised_at: str
    exception_message: str
    exception_trace: str


class DlqOutputMessagePayload(BaseModel):
    message: Any
    failure: list[FailedMessageData]
