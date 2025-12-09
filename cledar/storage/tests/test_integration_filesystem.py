# mypy: disable-error-code=no-untyped-def
import io
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from faker import Faker

from cledar.storage.exceptions import ReadFileError
from cledar.storage.models import ObjectStorageServiceConfig
from cledar.storage.object_storage import ObjectStorageService

fake = Faker()


@pytest.fixture(scope="module")
def object_storage_service() -> ObjectStorageService:
    """
    Create an ObjectStorageService with minimal S3 config (only for local operations).
    """
    # we still need to provide S3 config, but it won't be used for local operations
    config = ObjectStorageServiceConfig(
        s3_endpoint_url="http://localhost:9000",
        s3_access_key="dummy",
        s3_secret_key="dummy",
        s3_max_concurrency=10,
    )
    return ObjectStorageService(config)


@pytest.fixture
def test_dir() -> Generator[str, None, None]:
    """
    Create a temporary directory and clean it up after test.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


def test_upload_and_read_buffer(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test uploading a buffer and reading it back.
    """
    test_content = fake.text().encode()
    buffer = io.BytesIO(test_content)
    file_path = f"{test_dir}/test_buffer_{fake.file_name()}"

    object_storage_service.upload_buffer(buffer=buffer, destination_path=file_path)

    result = object_storage_service.read_file(path=file_path)
    assert result == test_content


def test_upload_buffer_invalid_params(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test that upload_buffer raises error with invalid params.
    """
    buffer = io.BytesIO(b"test")
    with pytest.raises(
        ValueError,
        match="Either destination_path or bucket and key must be provided",
    ):
        object_storage_service.upload_buffer(buffer=buffer)


def test_upload_and_download_file(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test uploading a file and downloading it.
    """
    test_content = fake.text().encode()
    source_path = f"{test_dir}/source_{fake.file_name()}"
    dest_path = f"{test_dir}/dest_{fake.file_name()}"

    with open(source_path, "wb") as f:
        f.write(test_content)
    upload_path = f"{test_dir}/uploaded_{fake.file_name()}"
    object_storage_service.upload_file(
        file_path=source_path, destination_path=upload_path
    )

    object_storage_service.download_file(dest_path=dest_path, source_path=upload_path)

    with open(dest_path, "rb") as f:
        downloaded_content = f.read()
    assert downloaded_content == test_content


def test_read_file_retry_mechanism(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test that read_file succeeds with valid file.
    """
    test_content = fake.text().encode()
    buffer = io.BytesIO(test_content)
    file_path = f"{test_dir}/retry_{fake.file_name()}"

    object_storage_service.upload_buffer(buffer=buffer, destination_path=file_path)

    result = object_storage_service.read_file(path=file_path, max_tries=3)
    assert result == test_content


def test_list_objects_recursive(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test listing objects recursively.
    """
    files = [
        "folder1/file1.txt",
        "folder1/file2.txt",
        "folder1/subfolder/file3.txt",
        "folder2/file4.txt",
    ]

    for file_path in files:
        full_path = f"{test_dir}/{file_path}"
        Path(full_path).parent.mkdir(parents=True, exist_ok=True)
        buffer = io.BytesIO(fake.text().encode())
        object_storage_service.upload_buffer(buffer=buffer, destination_path=full_path)

    result = object_storage_service.list_objects(path=test_dir, recursive=True)

    result_basenames = [Path(r).relative_to(test_dir).as_posix() for r in result]

    assert len(result_basenames) >= 4
    for file_path in files:
        assert file_path in result_basenames


def test_list_objects_with_prefix(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test listing objects with prefix filter.
    """
    files = [
        "prefix1/file1.txt",
        "prefix1/file2.txt",
        "prefix2/file3.txt",
    ]

    for file_path in files:
        full_path = f"{test_dir}/{file_path}"
        Path(full_path).parent.mkdir(parents=True, exist_ok=True)
        buffer = io.BytesIO(fake.text().encode())
        object_storage_service.upload_buffer(buffer=buffer, destination_path=full_path)

    prefix1_dir = f"{test_dir}/prefix1"
    result = object_storage_service.list_objects(path=prefix1_dir, recursive=True)

    result_basenames = [Path(r).name for r in result]

    assert len(result_basenames) >= 2
    assert "file1.txt" in result_basenames
    assert "file2.txt" in result_basenames
    assert "file3.txt" not in result_basenames


def test_list_objects_non_recursive(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test listing objects non-recursively.
    """
    files = [
        "root1.txt",
        "root2.txt",
        "folder/nested.txt",
    ]

    for file_path in files:
        full_path = f"{test_dir}/{file_path}"
        Path(full_path).parent.mkdir(parents=True, exist_ok=True)
        buffer = io.BytesIO(fake.text().encode())
        object_storage_service.upload_buffer(buffer=buffer, destination_path=full_path)

    result = object_storage_service.list_objects(path=test_dir, recursive=False)

    result_basenames = [Path(r).name for r in result]

    assert len(result) >= 2
    has_root_file = any("root" in r for r in result_basenames)
    has_folder = any("folder" in r for r in result_basenames)
    assert has_root_file or has_folder


def test_file_exists(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test checking if file exists.
    """
    file_path = f"{test_dir}/exists_{fake.file_name()}"

    assert object_storage_service.file_exists(path=file_path) is False

    buffer = io.BytesIO(fake.text().encode())
    object_storage_service.upload_buffer(buffer=buffer, destination_path=file_path)

    assert object_storage_service.file_exists(path=file_path) is True


def test_delete_file(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test deleting a file.
    """
    file_path = f"{test_dir}/delete_{fake.file_name()}"

    buffer = io.BytesIO(fake.text().encode())
    object_storage_service.upload_buffer(buffer=buffer, destination_path=file_path)

    assert object_storage_service.file_exists(path=file_path) is True

    object_storage_service.delete_file(path=file_path)

    assert object_storage_service.file_exists(path=file_path) is False


def test_get_file_size(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test getting file size.
    """
    test_content = fake.text().encode()
    file_path = f"{test_dir}/size_{fake.file_name()}"

    buffer = io.BytesIO(test_content)
    object_storage_service.upload_buffer(buffer=buffer, destination_path=file_path)

    size = object_storage_service.get_file_size(path=file_path)
    assert size == len(test_content)


def test_get_file_info(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test getting file metadata.
    """
    test_content = fake.text().encode()
    file_path = f"{test_dir}/info_{fake.file_name()}"

    buffer = io.BytesIO(test_content)
    object_storage_service.upload_buffer(buffer=buffer, destination_path=file_path)

    info = object_storage_service.get_file_info(path=file_path)

    assert "size" in info
    assert info["size"] == len(test_content)
    assert info is not None


def test_copy_file(object_storage_service: ObjectStorageService, test_dir: str) -> None:
    """
    Test copying a file.
    """
    test_content = fake.text().encode()
    source_path = f"{test_dir}/copy_source_{fake.file_name()}"
    dest_path = f"{test_dir}/copy_dest_{fake.file_name()}"

    buffer = io.BytesIO(test_content)
    object_storage_service.upload_buffer(buffer=buffer, destination_path=source_path)

    object_storage_service.copy_file(source_path=source_path, dest_path=dest_path)

    assert object_storage_service.file_exists(path=source_path) is True
    assert object_storage_service.file_exists(path=dest_path) is True

    dest_content = object_storage_service.read_file(path=dest_path)
    assert dest_content == test_content


def test_move_file(object_storage_service: ObjectStorageService, test_dir: str) -> None:
    """
    Test moving a file.
    """
    test_content = fake.text().encode()
    source_path = f"{test_dir}/move_source_{fake.file_name()}"
    dest_path = f"{test_dir}/move_dest_{fake.file_name()}"

    buffer = io.BytesIO(test_content)
    object_storage_service.upload_buffer(buffer=buffer, destination_path=source_path)

    object_storage_service.move_file(source_path=source_path, dest_path=dest_path)

    assert object_storage_service.file_exists(path=source_path) is False
    assert object_storage_service.file_exists(path=dest_path) is True

    dest_content = object_storage_service.read_file(path=dest_path)
    assert dest_content == test_content


def test_read_nonexistent_file(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test reading a file that doesn't exist.
    """
    non_existent_path = f"{test_dir}/nonexistent_{str(fake.uuid4())}.txt"

    with pytest.raises(ReadFileError):
        object_storage_service.read_file(path=non_existent_path, max_tries=1)


def test_invalid_parameters(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test operations with invalid parameters.
    """
    with pytest.raises(
        ValueError, match="Either path or bucket and key must be provided"
    ):
        object_storage_service.read_file()

    with pytest.raises(
        ValueError,
        match="Either destination_path or bucket and key must be provided",
    ):
        object_storage_service.upload_buffer(buffer=io.BytesIO(b"test"))

    with pytest.raises(
        ValueError,
        match="Either source_path or source_bucket and source_key must be provided",
    ):
        object_storage_service.copy_file(dest_path="/tmp/test")


def test_large_file_upload_download(
    object_storage_service: ObjectStorageService, test_dir: str
) -> None:
    """
    Test uploading and downloading a larger file (10MB).
    """
    size_mb = 10
    test_content = fake.binary(length=size_mb * 1024 * 1024)
    file_path = f"{test_dir}/large_{fake.file_name()}"

    buffer = io.BytesIO(test_content)
    object_storage_service.upload_buffer(buffer=buffer, destination_path=file_path)

    size = object_storage_service.get_file_size(path=file_path)
    assert size == len(test_content)

    result = object_storage_service.read_file(path=file_path)
    assert result == test_content
    assert len(result) == size_mb * 1024 * 1024
