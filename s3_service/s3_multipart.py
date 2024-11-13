import logging
from typing import Any
from uuid import uuid4
from threading import Lock

from .models.upload_response_dto import UploadResponseDto
from .models.s3_part import S3Part
from .s3 import S3Service
from .models.multipart_session_dto import MultipartSessionDto
from .models.upload_chunk_dto import UploadChunkDto
from .models.upload_status import UploadStatus

logger = logging.getLogger("s3_service")


class S3MultipartService(S3Service):
    multipart_sessions: dict[str, MultipartSessionDto] = {}
    _session_lock = Lock()

    def upload_file_chunk(self, dto: UploadChunkDto) -> UploadResponseDto:
        """Upload a file in chunks to S3."""
        try:
            session = self._get_multipart_session(dto)

            # Upload the part
            part = self._upload_part(
                session=session,
                part_number=dto.chunk_no,
                body=dto.body,
            )

            with session.lock:
                session.parts.append(part)
                session.uploaded_chunk_numbers.add(dto.chunk_no)

                # Check if all chunks have been uploaded
                if len(session.uploaded_chunk_numbers) == session.total_chunks:
                    # All chunks uploaded
                    self._complete_chunk_upload(
                        session.bucket,
                        session.gen_filename,
                        session.upload_id,
                        dto.session_id,
                    )
                    return UploadResponseDto(
                        status=UploadStatus.COMPLETE,
                        gen_filename=session.gen_filename,
                        file_name=session.file_name,
                        chunk_number=dto.chunk_no,
                        total_chunks=dto.total_chunks,
                    )

            return UploadResponseDto(
                status=UploadStatus.CHUNK_RECEIVED,
                chunk_number=dto.chunk_no,
                file_name=session.file_name,
                total_chunks=dto.total_chunks,
                session_id=dto.session_id,
            )

        except Exception as exception:
            logger.exception("Failed to upload file chunk")
            self._abort_upload_session(dto.session_id)
            raise exception

    def _get_multipart_session(self, dto: UploadChunkDto) -> MultipartSessionDto:
        """Get multipart session by session ID."""

        # Check if the chunk number is valid
        if dto.chunk_no > dto.total_chunks or dto.chunk_no <= 0:
            raise ValueError("Invalid chunk number")

        with self._session_lock:
            if dto.session_id not in self.multipart_sessions:
                self._init_upload_session(dto)

            session = self.multipart_sessions[dto.session_id]

        return session

    def _init_upload_session(self, dto: UploadChunkDto) -> None:
        """Initialize a multipart upload for a file."""
        try:
            gen_filename = f"{uuid4().hex}.sav"
            logger.debug(
                "Initiating upload in chunks",
                extra={"file_name": dto.file_name, "bucket": dto.bucket},
            )

            response = self.client.create_multipart_upload(
                Bucket=dto.bucket, Key=gen_filename
            )

            session = MultipartSessionDto(
                gen_filename=gen_filename,
                file_name=dto.file_name,
                bucket=dto.bucket,
                parts=[],
                upload_id=response["UploadId"],
                total_chunks=dto.total_chunks,
            )

            self.multipart_sessions[dto.session_id] = session

            logger.debug(
                "Upload initiated",
                extra={
                    "file_name": dto.file_name,
                    "bucket": dto.bucket,
                    "gen_filename": gen_filename,
                },
            )
        except Exception as exception:
            logger.exception(
                "Failed to initiate upload", extra={"session_id": dto.session_id}
            )
            raise exception

    def _upload_part(
        self, session: MultipartSessionDto, part_number: int, body: Any
    ) -> S3Part:
        """Upload a single part to S3."""

        logger.debug(
            "Uploading chunk part",
            extra={"file_name": session.file_name, "part_number": part_number},
        )
        response = self.client.upload_part(
            Bucket=session.bucket,
            Key=session.gen_filename,
            PartNumber=part_number,
            UploadId=session.upload_id,
            Body=body,
        )
        logger.debug(
            "Uploaded chunk part",
            extra={
                "key": session.file_name,
                "part_number": part_number,
                "upload_id": session.upload_id,
            },
        )

        return S3Part(PartNumber=part_number, ETag=response["ETag"])

    def _complete_chunk_upload(
        self, bucket: str, key: str, upload_id: str, session_id: str
    ) -> None:
        """Complete multipart upload by assembling the uploaded parts."""
        try:
            session = self.multipart_sessions[session_id]
            logger.debug(
                "Completing upload in chunks",
                extra={"file_name": key, "bucket": bucket, "upload_id": upload_id},
            )

            with session.lock:
                uploaded_parts = sorted(session.parts, key=lambda p: p["PartNumber"])
                parts_info = [
                    {"PartNumber": part["PartNumber"], "ETag": part["ETag"]}
                    for part in uploaded_parts
                ]
                self.client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts_info},
                )

            logger.debug(
                "Upload completed in chunks",
                extra={"file_name": key, "bucket": bucket, "upload_id": upload_id},
            )
        except Exception as exception:
            logger.exception(
                "Failed to complete upload", extra={"session_id": session_id}
            )
            raise exception
        finally:
            with self._session_lock:
                del self.multipart_sessions[session_id]

    def _abort_upload_session(self, session_id: str) -> None:
        """Abort multipart upload in case of errors."""
        try:
            logger.debug("Aborting upload", extra={"session_id": session_id})

            with self._session_lock:
                session = self.multipart_sessions.get(session_id)

            if session:
                self.client.abort_multipart_upload(
                    Bucket=session.bucket,
                    Key=session.gen_filename,
                    UploadId=session.upload_id,
                )
                logger.info("Upload aborted", extra={"session_id": session_id})
        except Exception as exception:
            logger.exception("Failed to abort upload", extra={"session_id": session_id})
            raise exception
        finally:
            with self._session_lock:
                if session_id in self.multipart_sessions:
                    del self.multipart_sessions[session_id]
