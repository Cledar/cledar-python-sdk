import io
import logging
from dataclasses import dataclass

import boto3
import botocore.exceptions

from .exceptions import RequiredBucketNotFoundException

logger = logging.getLogger("s3_service")


@dataclass
class S3ServiceConfig:
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str


class S3Service:
    client: any = None

    def __init__(self, config: S3ServiceConfig) -> None:
        self.client = boto3.client(
            "s3",
            endpoint_url=config.s3_endpoint_url,
            aws_access_key_id=config.s3_access_key,
            aws_secret_access_key=config.s3_secret_key,
        )
        logger.info("Initiated client", extra={"endpoint_url": config.s3_endpoint_url})

    def has_bucket(self, bucket: str, throw=False) -> bool:
        try:
            self.client.head_bucket(
                Bucket=bucket,
            )
            return True
        except botocore.exceptions.ClientError as exception:
            if throw:
                logger.exception("Bucket not found", extra={"bucket": bucket})
                raise RequiredBucketNotFoundException from exception
            return False

    def upload_buffer(self, buffer: io.BytesIO, bucket: str, key: str) -> None:
        try:
            logger.debug(
                "Uploading file from buffer", extra={"bucket": bucket, "key": key}
            )
            self.client.upload_fileobj(
                Fileobj=buffer,
                Bucket=bucket,
                Key=key,
            )
            logger.debug(
                "Uploaded file from buffer", extra={"bucket": bucket, "key": key}
            )
        except Exception as exception:
            logger.exception(
                "Failed to upload buffer to S3",
            )
            raise exception

    def upload_file(self, file_path: str, bucket: str, key: str) -> None:
        try:
            logger.debug(
                "Uploading file from filesystem",
                extra={"bucket": bucket, "key": key, "file_path": file_path},
            )
            self.client.upload_file(
                Filename=file_path,
                Bucket=bucket,
                Key=key,
            )
            logger.debug(
                "Uploaded file from filesystem",
                extra={"bucket": bucket, "key": key, "file_path": file_path},
            )
        except Exception as exception:
            logger.exception(
                "Failed to upload file to S3",
            )
            raise exception
