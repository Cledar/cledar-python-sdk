# mypy: disable-error-code=method-assign
from unittest.mock import MagicMock, patch
import pytest
from faker import Faker
from s3_service.models.upload_status import UploadStatus
from s3_service.s3 import S3ServiceConfig
from s3_service.models.upload_chunk_dto import UploadChunkDto
from s3_service.s3_multipart import S3MultipartService

fake = Faker()


@pytest.fixture(name="s3_multipart_service")
@patch("boto3.client")
def fixture_s3_service(
    client: MagicMock, s3_config: S3ServiceConfig
) -> S3MultipartService:
    client.return_value(MagicMock())
    return S3MultipartService(s3_config)


def test_upload_file_chunk(s3_multipart_service: S3MultipartService) -> None:
    # Arrange
    session_id = str(fake.uuid4())
    dto = UploadChunkDto(
        bucket=fake.name(),
        body=b"test data",
        chunk_no=1,
        total_chunks=2,
        file_name=fake.file_name(),
        session_id=session_id,
    )

    # Mock the necessary S3 client responses
    with patch.object(
        s3_multipart_service.client,
        "create_multipart_upload",
        return_value={"UploadId": "test-upload-id"},
    ), patch.object(
        s3_multipart_service.client,
        "upload_part",
        return_value={"PartNumber": dto.chunk_no, "ETag": "test-etag"},
    ), patch.object(
        s3_multipart_service.client,
        "complete_multipart_upload",
        return_value={"status": "complete"},
    ):
        # Act: Uploading first chunk
        dto.chunk_no = 1
        result = s3_multipart_service.upload_file_chunk(dto)

        # Assert: First chunk upload
        assert result.status == UploadStatus.CHUNK_RECEIVED
        assert result.chunk_number == dto.chunk_no
        assert result.total_chunks == dto.total_chunks

        # Act: Uploading the last chunk
        dto.chunk_no = 2
        result = s3_multipart_service.upload_file_chunk(dto)

        # Assert: Upload completion
        assert result.status == UploadStatus.COMPLETE
        assert result.chunk_number == dto.chunk_no
        assert result.total_chunks == dto.total_chunks
        assert result.gen_filename is not None
        assert result.file_name is not None
