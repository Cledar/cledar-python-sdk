from typing import Literal

from pydantic import BaseModel


class ObjectStorageServiceConfig(BaseModel):
    # s3 configuration
    s3_endpoint_url: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_max_concurrency: int | None = None
    # azure configuration
    azure_account_name: str | None = None
    azure_account_key: str | None = None


class TransferPath(BaseModel):
    backend: Literal["s3", "abfs", "local"]
    path: str
