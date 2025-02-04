from typing import Any
import pydantic


class FailedMessageData(pydantic.BaseModel):
    raised_at: str
    exception_message: str | None
    exception_trace: str | None


class DlqOutputMessagePayload(pydantic.BaseModel):
    message: Any


class DlqFailedFeaturePayload(DlqOutputMessagePayload):
    failed_feature: str | None


class FailedFeatureData(FailedMessageData):
    failed_feature: str | None
