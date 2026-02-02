"""Output models for Kafka module."""

import pydantic


class FailedMessageData(pydantic.BaseModel):
    """Data structure for recording message processing failures."""

    raised_at: str
    exception_message: str | None
    exception_trace: str | None
    failure_reason: str | None
