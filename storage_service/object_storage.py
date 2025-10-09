import io
import logging
import socket
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4
import fsspec


from .exceptions import RequiredBucketNotFoundException

logger = logging.getLogger("object_storage_service")


@dataclass
class ObjectStorageServiceConfig:
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str


class ObjectStorageService:
    client: Any = None

    def __init__(self, config: ObjectStorageServiceConfig) -> None:
        self.client = fsspec.filesystem(
            "s3",
            key=config.s3_access_key,
            secret=config.s3_secret_key,
            client_kwargs={"endpoint_url": config.s3_endpoint_url},
        )
        logger.info(
            "Initiated filesystem", extra={"endpoint_url": config.s3_endpoint_url}
        )
        self.local_client = fsspec.filesystem("file")

    def is_alive(self) -> bool:
        try:
            self.client.ls(path="")
            return True
        except Exception:  # pylint: disable=broad-exception-caught
            return False

    def has_bucket(self, bucket: str, throw: bool = False) -> bool:
        try:
            self.client.ls(path=bucket)
            return True
        except Exception as exception:  # pylint: disable=broad-exception-caught
            if throw:
                logger.exception("Bucket not found", extra={"bucket": bucket})
                raise RequiredBucketNotFoundException from exception
            return False

    def upload_buffer(
        self,
        buffer: io.BytesIO,
        bucket: str = None,
        key: str = None,
        destination_path: str = None,
    ) -> None:
        try:
            if destination_path:
                logger.debug(
                    "Uploading file from buffer",
                    extra={"destination_path": destination_path},
                )
                buffer.seek(0)
                with self.local_client.open(path=destination_path, mode="wb") as fobj:
                    fobj.write(buffer.getbuffer())
            elif bucket and key:
                buffer.seek(0)
                with self.client.open(path=f"s3://{bucket}/{key}", mode="wb") as fobj:
                    fobj.write(buffer.getbuffer())
                logger.debug(
                    "Uploaded file from buffer", extra={"bucket": bucket, "key": key}
                )
            else:
                raise ValueError(
                    "Either destination_path or bucket and key must be provided"
                )
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.exception("Failed to upload buffer to S3")
            raise exception

    def read_file(
        self, bucket: str = None, key: str = None, path: str = None, max_tries: int = 3
    ) -> bytes:
        for attempt in range(max_tries):
            try:
                if path:
                    logger.debug(
                        "Reading file from local filesystem", extra={"path": path}
                    )
                    with self.local_client.open(path=path, mode="rb") as fobj:
                        content: bytes = fobj.read()
                    logger.debug(
                        "File read from local filesystem", extra={"path": path}
                    )
                    return content
                if bucket and key:
                    logger.debug(
                        "Reading file from S3", extra={"bucket": bucket, "key": key}
                    )
                    with self.client.open(
                        path=f"s3://{bucket}/{key}", mode="rb"
                    ) as fobj:
                        content: bytes = fobj.read()
                    logger.debug(
                        "File read from S3", extra={"bucket": bucket, "key": key}
                    )
                    return content
                else:
                    raise ValueError("Either path or bucket and key must be provided")
            except (OSError, socket.error) as exception:
                if attempt == max_tries - 1:
                    logger.exception(
                        "Failed to read file after %d retries",
                        max_tries,
                        extra={"bucket": bucket, "key": key},
                    )
                    raise exception
                logger.warning(
                    "Failed to read file, retrying...",
                    extra={"bucket": bucket, "key": key},
                )
        raise NotImplementedError("This should never be reached")

    def upload_file(
        self,
        file_path: str,
        bucket: str = None,
        key: str = None,
        destination_path: str = None,
    ) -> None:
        try:
            if destination_path:
                logger.debug(
                    "Uploading file from filesystem to local filesystem",
                    extra={"destination_path": destination_path},
                )
                self.client.put(lpath=file_path, rpath=destination_path)
                logger.debug(
                    "Uploaded file from filesystem to local filesystem",
                    extra={"destination_path": destination_path},
                )
            elif bucket and key:
                logger.debug(
                    "Uploading file from filesystem to S3",
                    extra={"bucket": bucket, "key": key},
                )
                self.client.put(lpath=file_path, rpath=f"s3://{bucket}/{key}")
                logger.debug(
                    "Uploaded file from filesystem to S3",
                    extra={"bucket": bucket, "key": key},
                )
            else:
                raise ValueError(
                    "Either destination_path or bucket and key must be provided"
                )
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.exception("Failed to upload file")
            raise exception

    def verify_and_upload_test_file(self, bucket: str, throw: bool = False) -> None:
        logger.info("Starting verification and upload of test file.")
        try:
            buckets = self.client.ls(path="")
        except Exception:  # pylint: disable=broad-exception-caught
            buckets = []
        logger.info("Number of buckets: %d", len(buckets))
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
        except Exception as exception:  # pylint: disable=broad-exception-caught
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
