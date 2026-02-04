"""Models for object storage configuration and transfer paths."""

from typing import Literal

from pydantic import BaseModel


class ObjectStorageServiceConfig(BaseModel):
    """Configuration for object storage service.

    Attributes:
        s3_endpoint_url: S3 endpoint URL for custom S3-compatible services.
        s3_access_key: S3 access key for authentication.
        s3_secret_key: S3 secret key for authentication.
        s3_max_concurrency: Maximum number of concurrent S3 operations.
        azure_account_name: Azure storage account name.
        azure_account_key: Azure storage account key.

    """

    # s3 configuration
    s3_endpoint_url: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_max_concurrency: int = 10
    # azure configuration
    azure_account_name: str | None = None
    azure_account_key: str | None = None


class TransferPath(BaseModel):
    """Transfer path information with backend type.

    Attributes:
        backend: Storage backend type (s3, abfs, or local).
        path: Full path to the file or object.

    """

    backend: Literal["s3", "abfs", "local"]
    path: str
