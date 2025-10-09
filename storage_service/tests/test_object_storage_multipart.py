# mypy: disable-error-code=method-assign
# pylint: disable=import-error
from unittest.mock import MagicMock, patch
import pytest
from faker import Faker
from storage_service.models.upload_status import UploadStatus  # type: ignore[import-not-found]
from storage_service.object_storage import ObjectStorageServiceConfig  # type: ignore[import-not-found]
from storage_service.models.upload_chunk_dto import UploadChunkDto  # type: ignore[import-not-found]
from storage_service.object_storage_multipart import ObjectStorageMultipartService  # type: ignore[import-not-found]

fake = Faker()


@pytest.fixture(name="object_storage_multipart_service")
@patch("fsspec.filesystem")
def fixture_object_storage_service(
    fsspec_client: MagicMock, object_storage_config: ObjectStorageServiceConfig
) -> ObjectStorageMultipartService:
    fsspec_client.return_value = MagicMock()
    return ObjectStorageMultipartService(object_storage_config)


def test_upload_file_chunk(object_storage_multipart_service: ObjectStorageMultipartService) -> None:
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

    # Act: Uploading first chunk
    dto.chunk_no = 1
    result = object_storage_multipart_service.upload_file_chunk(dto)

    # Assert: First chunk upload
    assert result.status == UploadStatus.CHUNK_RECEIVED
    assert result.chunk_number == dto.chunk_no
    assert result.total_chunks == dto.total_chunks

    # Act: Uploading the last chunk
    dto.chunk_no = 2
    result = object_storage_multipart_service.upload_file_chunk(dto)

    # Assert: Upload completion
    assert result.status == UploadStatus.COMPLETE
    assert result.chunk_number == dto.chunk_no
    assert result.total_chunks == dto.total_chunks
    assert result.gen_filename is not None
    assert result.file_name is not None
