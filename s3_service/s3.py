import io
import logging
import socket
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

import boto3
import botocore.exceptions
from botocore.response import StreamingBody

from .exceptions import RequiredBucketNotFoundException

logger = logging.getLogger("s3_service")


@dataclass
class S3ServiceConfig:
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str


class S3Service:
    client: Any = None

    def __init__(self, config: S3ServiceConfig) -> None:
        self.client = boto3.client(
            "s3",
            endpoint_url=config.s3_endpoint_url,
            aws_access_key_id=config.s3_access_key,
            aws_secret_access_key=config.s3_secret_key,
        )
        logger.info("Initiated client", extra={"endpoint_url": config.s3_endpoint_url})

    def is_alive(self) -> bool:
        try:
            self.client.list_buckets()
            return True
        except Exception:  # pylint: disable=broad-exception-caught
            return False

    def has_bucket(self, bucket: str, throw: bool = False) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket)
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
            self.client.upload_fileobj(Fileobj=buffer, Bucket=bucket, Key=key)
            logger.debug(
                "Uploaded file from buffer", extra={"bucket": bucket, "key": key}
            )
        except Exception as exception:
            logger.exception("Failed to upload buffer to S3")
            raise exception

    def read_file(self, bucket: str, key: str, max_tries: int = 3) -> bytes:
        logger.debug("Reading file from S3...", extra={"bucket": bucket, "key": key})
        for attempt in range(max_tries):
            try:
                response: dict[str, Any] = self.client.get_object(
                    Bucket=bucket, Key=key
                )
                content_body: StreamingBody = response["Body"]
                content: bytes = content_body.read()
                logger.debug("File read from S3", extra={"bucket": bucket, "key": key})
                return content
            except (botocore.exceptions.IncompleteReadError, socket.error) as exception:
                if attempt == max_tries - 1:
                    logger.exception(
                        f"Failed to read file from S3 after {max_tries} retries",
                        extra={"bucket": bucket, "key": key},
                    )
                    raise exception
                logger.warning(
                    "Failed to read file from S3, retrying...",
                    extra={"bucket": bucket, "key": key},
                )
        raise NotImplementedError("This should never be reached")

    def upload_file(self, file_path: str, bucket: str, key: str) -> None:
        try:
            logger.debug(
                "Uploading file from filesystem",
                extra={"bucket": bucket, "key": key, "file_path": file_path},
            )
            self.client.upload_file(Filename=file_path, Bucket=bucket, Key=key)
            logger.debug(
                "Uploaded file from filesystem",
                extra={"bucket": bucket, "key": key, "file_path": file_path},
            )
        except Exception as exception:
            logger.exception("Failed to upload file to S3")
            raise exception

    def verify_and_upload_test_file(self, bucket: str, throw: bool = False) -> None:
        logger.info("Starting verification and upload of test file.")
        buckets = self.client.list_buckets().get("Buckets", [])
        logger.info(f"Number of buckets: {len(buckets)}")
        buffer = io.BytesIO()
        try:
            buffer.write(f"test contents {datetime.now()}".encode("utf-8"))
            buffer.seek(0)
            logger.info("Buffer created and written to.")
            self.has_bucket(bucket, throw=throw)
            test_key = uuid4().hex
            self.upload_buffer(
                buffer=buffer,
                bucket=bucket,
                key=test_key,
            )
            self.read_file(bucket=bucket, key=test_key)
        except Exception as exception:
            logger.error(
                "An error occurred during the verification and upload process",
                extra={
                    "exception": str(exception),
                    "trace": traceback.format_exc(),
                },
            )
            raise exception
        finally:
            buffer.close()
            logger.info("Buffer closed.")

        logger.info("Verification and upload of test file completed.")
