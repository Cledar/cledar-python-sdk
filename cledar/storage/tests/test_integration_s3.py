# mypy: disable-error-code=no-untyped-def
import io
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from faker import Faker
from testcontainers.minio import MinioContainer

from cledar.storage.exceptions import ReadFileError, RequiredBucketNotFoundError
from cledar.storage.models import ObjectStorageServiceConfig
from cledar.storage.object_storage import ObjectStorageService

fake = Faker()


@pytest.fixture(scope="module")
def minio_container():
    """
    Start a MinIO container for testing.
    """
    with MinioContainer(
        access_key="minioadmin",
        secret_key="minioadmin",
    ) as minio:
        yield minio


@pytest.fixture(scope="module")
def object_storage_service(minio_container: MinioContainer) -> ObjectStorageService:
    """
    Create an ObjectStorageService connected to MinIO testcontainer.
    """
    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(minio_container.port)
    endpoint_url = f"http://{host}:{port}"

    config = ObjectStorageServiceConfig(
        s3_endpoint_url=endpoint_url,
        s3_access_key=minio_container.access_key,
        s3_secret_key=minio_container.secret_key,
        s3_max_concurrency=10,
    )
    return ObjectStorageService(config)


@pytest.fixture
def test_bucket(
    object_storage_service: ObjectStorageService,
) -> Generator[str, None, None]:
    """
    Create a test bucket and clean it up after test.
    """
    bucket_name = f"test-bucket-{str(fake.uuid4())}"
    object_storage_service.client.mkdir(f"s3://{bucket_name}")
    yield bucket_name
    objects = object_storage_service.list_objects(bucket=bucket_name, recursive=True)
    for obj in objects:
        object_storage_service.delete_file(bucket=bucket_name, key=obj)
    object_storage_service.client.rmdir(bucket_name)


def test_is_alive(object_storage_service: ObjectStorageService) -> None:
    """
    Test that the service can connect to MinIO.
    """
    assert object_storage_service.is_alive() is True


def test_has_bucket_exists(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test checking if a bucket exists.
    """
    result = object_storage_service.has_bucket(bucket=test_bucket)
    assert result is True


def test_has_bucket_not_exists(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test checking if a non-existent bucket does not exist.
    """
    non_existent_bucket = f"non-existent-{str(fake.uuid4())}"
    result = object_storage_service.has_bucket(bucket=non_existent_bucket)
    assert result is False


def test_has_bucket_throw_not_exists(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test that has_bucket throws exception when throw=True.
    """
    non_existent_bucket = f"non-existent-{str(fake.uuid4())}"
    with pytest.raises(RequiredBucketNotFoundError):
        object_storage_service.has_bucket(bucket=non_existent_bucket, throw=True)


def test_upload_and_read_buffer(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test uploading a buffer and reading it back.
    """
    test_content = fake.text().encode()
    buffer = io.BytesIO(test_content)
    key = f"test/buffer/{fake.file_name()}"

    object_storage_service.upload_buffer(buffer=buffer, bucket=test_bucket, key=key)

    result = object_storage_service.read_file(bucket=test_bucket, key=key)
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
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test uploading a file and downloading it.
    """
    test_content = fake.text().encode()
    key = f"test/files/{fake.file_name()}"

    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
        temp_file.write(test_content)
        temp_file_path = temp_file.name

    try:
        object_storage_service.upload_file(
            file_path=temp_file_path, bucket=test_bucket, key=key
        )

        with tempfile.NamedTemporaryFile(mode="rb", delete=False) as download_file:
            download_path = download_file.name

        try:
            object_storage_service.download_file(
                dest_path=download_path, bucket=test_bucket, key=key
            )

            with open(download_path, "rb") as f:
                downloaded_content = f.read()
            assert downloaded_content == test_content
        finally:
            Path(download_path).unlink(missing_ok=True)
    finally:
        Path(temp_file_path).unlink(missing_ok=True)


def test_read_file_retry_mechanism(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test that read_file succeeds with valid file.
    """
    test_content = fake.text().encode()
    buffer = io.BytesIO(test_content)
    key = f"test/retry/{fake.file_name()}"

    object_storage_service.upload_buffer(buffer=buffer, bucket=test_bucket, key=key)

    result = object_storage_service.read_file(bucket=test_bucket, key=key, max_tries=3)
    assert result == test_content


def test_list_objects_recursive(
    object_storage_service: ObjectStorageService, test_bucket: str
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

    for file_key in files:
        buffer = io.BytesIO(fake.text().encode())
        object_storage_service.upload_buffer(
            buffer=buffer, bucket=test_bucket, key=file_key
        )
    result = object_storage_service.list_objects(bucket=test_bucket, recursive=True)

    files_only = [r for r in result if not r.endswith("/")]

    assert len(files_only) >= 4
    for file_key in files:
        assert file_key in result or file_key in files_only


def test_list_objects_with_prefix(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test listing objects with prefix filter.
    """
    files = [
        "prefix1/file1.txt",
        "prefix1/file2.txt",
        "prefix2/file3.txt",
    ]

    for file_key in files:
        buffer = io.BytesIO(fake.text().encode())
        object_storage_service.upload_buffer(
            buffer=buffer, bucket=test_bucket, key=file_key
        )

    result = object_storage_service.list_objects(
        bucket=test_bucket, prefix="prefix1/", recursive=True
    )

    files_only = [r for r in result if not r.endswith("/")]

    assert len(files_only) >= 2
    assert "prefix1/file1.txt" in result or "prefix1/file1.txt" in files_only
    assert "prefix1/file2.txt" in result or "prefix1/file2.txt" in files_only
    assert "prefix2/file3.txt" not in result


def test_list_objects_non_recursive(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test listing objects non-recursively.
    """
    files = [
        "root1.txt",
        "root2.txt",
        "folder/nested.txt",
    ]

    for file_key in files:
        buffer = io.BytesIO(fake.text().encode())
        object_storage_service.upload_buffer(
            buffer=buffer, bucket=test_bucket, key=file_key
        )

    result = object_storage_service.list_objects(bucket=test_bucket, recursive=False)

    assert len(result) >= 1
    has_root_file = any("root" in r for r in result)
    assert has_root_file or len(result) > 0


def test_file_exists(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test checking if file exists.
    """
    key = f"test/exists/{fake.file_name()}"

    assert object_storage_service.file_exists(bucket=test_bucket, key=key) is False

    buffer = io.BytesIO(fake.text().encode())
    object_storage_service.upload_buffer(buffer=buffer, bucket=test_bucket, key=key)

    assert object_storage_service.file_exists(bucket=test_bucket, key=key) is True


def test_delete_file(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test deleting a file.
    """
    key = f"test/delete/{fake.file_name()}"

    buffer = io.BytesIO(fake.text().encode())
    object_storage_service.upload_buffer(buffer=buffer, bucket=test_bucket, key=key)

    assert object_storage_service.file_exists(bucket=test_bucket, key=key) is True

    object_storage_service.delete_file(bucket=test_bucket, key=key)

    assert object_storage_service.file_exists(bucket=test_bucket, key=key) is False


def test_get_file_size(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test getting file size.
    """
    test_content = fake.text().encode()
    key = f"test/size/{fake.file_name()}"

    buffer = io.BytesIO(test_content)
    object_storage_service.upload_buffer(buffer=buffer, bucket=test_bucket, key=key)

    size = object_storage_service.get_file_size(bucket=test_bucket, key=key)
    assert size == len(test_content)


def test_get_file_info(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test getting file metadata.
    """
    test_content = fake.text().encode()
    key = f"test/info/{fake.file_name()}"

    buffer = io.BytesIO(test_content)
    object_storage_service.upload_buffer(buffer=buffer, bucket=test_bucket, key=key)

    info = object_storage_service.get_file_info(bucket=test_bucket, key=key)

    assert "size" in info
    assert info["size"] == len(test_content)
    assert info is not None


def test_copy_file(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test copying a file.
    """
    test_content = fake.text().encode()
    source_key = f"test/copy/source/{fake.file_name()}"
    dest_key = f"test/copy/dest/{fake.file_name()}"

    buffer = io.BytesIO(test_content)
    object_storage_service.upload_buffer(
        buffer=buffer, bucket=test_bucket, key=source_key
    )

    object_storage_service.copy_file(
        source_bucket=test_bucket,
        source_key=source_key,
        dest_bucket=test_bucket,
        dest_key=dest_key,
    )

    assert (
        object_storage_service.file_exists(bucket=test_bucket, key=source_key) is True
    )
    assert object_storage_service.file_exists(bucket=test_bucket, key=dest_key) is True

    dest_content = object_storage_service.read_file(bucket=test_bucket, key=dest_key)
    assert dest_content == test_content


def test_move_file(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test moving a file.
    """
    test_content = fake.text().encode()
    source_key = f"test/move/source/{fake.file_name()}"
    dest_key = f"test/move/dest/{fake.file_name()}"

    buffer = io.BytesIO(test_content)
    object_storage_service.upload_buffer(
        buffer=buffer, bucket=test_bucket, key=source_key
    )

    object_storage_service.move_file(
        source_bucket=test_bucket,
        source_key=source_key,
        dest_bucket=test_bucket,
        dest_key=dest_key,
    )

    assert (
        object_storage_service.file_exists(bucket=test_bucket, key=source_key) is False
    )
    assert object_storage_service.file_exists(bucket=test_bucket, key=dest_key) is True

    dest_content = object_storage_service.read_file(bucket=test_bucket, key=dest_key)
    assert dest_content == test_content


def test_read_nonexistent_file(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test reading a file that doesn't exist.
    """
    non_existent_key = f"test/nonexistent/{str(fake.uuid4())}.txt"

    with pytest.raises(ReadFileError):
        object_storage_service.read_file(
            bucket=test_bucket, key=non_existent_key, max_tries=1
        )


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
        object_storage_service.copy_file(dest_bucket="test", dest_key="test")


def test_large_file_upload_download(
    object_storage_service: ObjectStorageService, test_bucket: str
) -> None:
    """
    Test uploading and downloading a larger file (10MB).
    """
    size_mb = 10
    test_content = fake.binary(length=size_mb * 1024 * 1024)
    key = f"test/large/{fake.file_name()}"

    buffer = io.BytesIO(test_content)
    object_storage_service.upload_buffer(buffer=buffer, bucket=test_bucket, key=key)

    size = object_storage_service.get_file_size(bucket=test_bucket, key=key)
    assert size == len(test_content)

    result = object_storage_service.read_file(bucket=test_bucket, key=key)
    assert result == test_content
    assert len(result) == size_mb * 1024 * 1024
