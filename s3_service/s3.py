from typing import Any
import io
import logging
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4
import traceback
import boto3
import botocore.exceptions
from stream_chunker.monitoring_service.chunker_metrics import (
    s3_requests,
    s3_requests_latency,
)
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

    def has_bucket(self, bucket: str, throw=False) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket)
            s3_requests.labels(method="has_bucket", status="success").inc()
            return True
        except botocore.exceptions.ClientError as exception:
            s3_requests.labels(method="has_bucket", status="failure").inc()
            if throw:
                logger.exception("Bucket not found", extra={"bucket": bucket})
                raise RequiredBucketNotFoundException from exception
            return False

    def upload_buffer(self, buffer: io.BytesIO, bucket: str, key: str) -> None:
        with s3_requests_latency.labels(method="upload_buffer").time():
            try:
                logger.debug(
                    "Uploading file from buffer", extra={"bucket": bucket, "key": key}
                )
                self.client.upload_fileobj(Fileobj=buffer, Bucket=bucket, Key=key)
                s3_requests.labels(method="upload_buffer", status="success").inc()
                logger.debug(
                    "Uploaded file from buffer", extra={"bucket": bucket, "key": key}
                )
            except Exception as exception:
                s3_requests.labels(method="upload_buffer", status="failure").inc()
                logger.exception("Failed to upload buffer to S3")
                raise exception

    def read_file(self, bucket: str, key: str) -> bytes:
        try:
            logger.debug("Reading file from S3", extra={"bucket": bucket, "key": key})
            response = self.client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read()
            logger.debug("Read file from S3", extra={"bucket": bucket, "key": key})
            return content
        except Exception as exception:
            logger.error(
                "Failed to read file from S3",
                extra={"exception": str(exception), "trace": traceback.format_exc()},
            )
            raise exception

    def upload_file(self, file_path: str, bucket: str, key: str) -> None:
        with s3_requests_latency.labels(method="upload_file").time():
            try:
                logger.debug(
                    "Uploading file from filesystem",
                    extra={"bucket": bucket, "key": key, "file_path": file_path},
                )
                self.client.upload_file(Filename=file_path, Bucket=bucket, Key=key)
                s3_requests.labels(method="upload_file", status="success").inc()
                logger.debug(
                    "Uploaded file from filesystem",
                    extra={"bucket": bucket, "key": key, "file_path": file_path},
                )
            except Exception as exception:
                s3_requests.labels(method="upload_file", status="failure").inc()
                logger.exception("Failed to upload file to S3")
                raise exception

    def verify_and_upload_test_file(self, bucket: str, throw: bool = False):
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
