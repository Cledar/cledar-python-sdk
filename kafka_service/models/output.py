import pydantic


class FailedMessageData(pydantic.BaseModel):
    raised_at: str
    exception_message: str | None
    exception_trace: str | None
    failure_reason: str | None
