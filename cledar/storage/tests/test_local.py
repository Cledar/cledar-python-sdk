# mypy: disable-error-code=no-untyped-def
import io
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker

from cledar.storage.exceptions import ReadFileError, UploadFileError
from cledar.storage.models import ObjectStorageServiceConfig
from cledar.storage.object_storage import ObjectStorageService

fake = Faker()


@pytest.fixture(name="object_storage_service")
@patch("fsspec.filesystem")
def fixture_object_storage_service(
    fsspec_client: MagicMock, object_storage_config: ObjectStorageServiceConfig
) -> ObjectStorageService:
    fsspec_client.return_value = MagicMock()
    return ObjectStorageService(object_storage_config)


@patch("fsspec.filesystem")
def test_init(
    fsspec_client: MagicMock, object_storage_config: ObjectStorageServiceConfig
) -> None:
    ObjectStorageService(object_storage_config)

    fsspec_client.assert_any_call(
        "s3",
        key=object_storage_config.s3_access_key,
        secret=object_storage_config.s3_secret_key,
        client_kwargs={"endpoint_url": object_storage_config.s3_endpoint_url},
        max_concurrency=object_storage_config.s3_max_concurrency,
    )
    fsspec_client.assert_any_call(
        "file",
    )


def test_upload_buffer_local_success(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test successful buffer upload to filesystem.
    """
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())
    destination_path = fake.file_path()

    mock_file = MagicMock()
    mock_file.write = MagicMock()

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    object_storage_service.local_client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    object_storage_service.upload_buffer(
        buffer=buffer_bytes, destination_path=destination_path
    )

    object_storage_service.local_client.open.assert_called_once_with(
        path=destination_path, mode="wb"
    )
    mock_file.write.assert_called_once_with(buffer_bytes.getbuffer())


def test_upload_buffer_local_exception(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test buffer upload to filesystem with write exception.
    """
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())
    destination_path = fake.file_path()

    class Writer:
        def write(self, _b: bytes) -> None:
            raise UploadFileError

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield Writer()

    object_storage_service.local_client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    with pytest.raises(UploadFileError):
        object_storage_service.upload_buffer(
            buffer=buffer_bytes, destination_path=destination_path
        )
    object_storage_service.local_client.open.assert_called_once()


def test_upload_buffer_local_missing_params(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test buffer upload with missing parameters.
    """
    buffer_str = io.StringIO(fake.text())
    buffer_bytes = io.BytesIO(buffer_str.getvalue().encode())

    with pytest.raises(
        ValueError, match="Either destination_path or bucket and key must be provided"
    ):
        object_storage_service.upload_buffer(buffer=buffer_bytes)


def test_read_file_local_success(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test successful file read from filesystem.
    """
    path = fake.file_path()
    expected_content = fake.text().encode()

    mock_file = MagicMock()
    mock_file.read.return_value = expected_content

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    object_storage_service.local_client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    result = object_storage_service.read_file(path=path)
    assert result == expected_content
    object_storage_service.local_client.open.assert_called_once_with(
        path=path, mode="rb"
    )


def test_read_file_local_retry_mechanism(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test file read retry mechanism on filesystem errors.
    """
    path = fake.file_path()
    expected_content = fake.text().encode()

    mock_file = MagicMock()
    mock_file.read.return_value = expected_content

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    call_count = 0

    def side_effect(*_args, **_kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise OSError("Temporary error")
        return open_cm()

    object_storage_service.local_client.open = MagicMock(side_effect=side_effect)

    result = object_storage_service.read_file(path=path, max_tries=3)

    assert result == expected_content
    assert call_count == 3


def test_read_file_local_max_retries_exceeded(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test file read when max retries are exceeded.
    """
    path = fake.file_path()

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        raise ReadFileError

    object_storage_service.local_client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    with pytest.raises(ReadFileError):
        object_storage_service.read_file(path=path, max_tries=2)


def test_read_file_local_missing_params(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test file read with missing parameters.
    """
    with pytest.raises(
        ValueError, match="Either path or bucket and key must be provided"
    ):
        object_storage_service.read_file()


def test_upload_file_local_success(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test successful file upload to filesystem.
    """
    file_path = fake.file_path()
    destination_path = fake.file_path()

    object_storage_service.local_client.put = MagicMock()

    object_storage_service.upload_file(
        file_path=file_path, destination_path=destination_path
    )

    object_storage_service.local_client.put.assert_called_once_with(
        lpath=file_path, rpath=destination_path
    )


def test_upload_file_local_exception(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test file upload to filesystem with exception.
    """
    file_path = fake.file_path()
    destination_path = fake.file_path()

    object_storage_service.local_client.put.side_effect = UploadFileError

    with pytest.raises(UploadFileError):
        object_storage_service.upload_file(
            file_path=file_path, destination_path=destination_path
        )

    object_storage_service.local_client.put.assert_called_once_with(
        lpath=file_path, rpath=destination_path
    )


def test_upload_file_local_missing_params(
    object_storage_service: ObjectStorageService,
) -> None:
    """
    Test file upload with missing parameters.
    """
    file_path = fake.file_path()

    with pytest.raises(
        ValueError, match="Either destination_path or bucket and key must be provided"
    ):
        object_storage_service.upload_file(file_path=file_path)


def test_list_objects_recursive_local(
    object_storage_service: ObjectStorageService,
) -> None:
    path = "/tmp/test"
    mock_objects = [
        "/tmp/test/file1.txt",
        "/tmp/test/file2.txt",
        "/tmp/test/subfolder/file3.txt",
    ]
    object_storage_service.local_client.find.return_value = mock_objects

    result = object_storage_service.list_objects(path=path, recursive=True)

    assert len(result) == 3
    assert "/tmp/test/file1.txt" in result
    object_storage_service.local_client.find.assert_called_once()


def test_list_objects_non_recursive_local(
    object_storage_service: ObjectStorageService,
) -> None:
    path = "/tmp/test"
    mock_objects = ["/tmp/test/file1.txt", "/tmp/test/file2.txt"]
    object_storage_service.local_client.ls.return_value = mock_objects

    result = object_storage_service.list_objects(path=path, recursive=False)

    assert len(result) == 2
    object_storage_service.local_client.ls.assert_called_once()


def test_delete_file_local(object_storage_service: ObjectStorageService) -> None:
    path = "/tmp/test/file.txt"

    object_storage_service.delete_file(path=path)

    object_storage_service.local_client.rm.assert_called_once_with(path)


def test_file_exists_true_local(object_storage_service: ObjectStorageService) -> None:
    path = "/tmp/test/file.txt"
    object_storage_service.local_client.exists.return_value = True

    result = object_storage_service.file_exists(path=path)

    assert result is True
    object_storage_service.local_client.exists.assert_called_once_with(path)


def test_file_exists_false_local(object_storage_service: ObjectStorageService) -> None:
    path = "/tmp/test/file.txt"
    object_storage_service.local_client.exists.return_value = False

    result = object_storage_service.file_exists(path=path)

    assert result is False
    object_storage_service.local_client.exists.assert_called_once_with(path)


def test_download_file_local(object_storage_service: ObjectStorageService) -> None:
    source_path = "/tmp/source/file.txt"
    dest_path = "/tmp/dest/file.txt"

    object_storage_service.download_file(dest_path, source_path=source_path)

    object_storage_service.local_client.get.assert_called_once_with(
        source_path, dest_path
    )


def test_get_file_size_local(object_storage_service: ObjectStorageService) -> None:
    path = "/tmp/test/file.txt"
    expected_size = 2048
    object_storage_service.local_client.info.return_value = {"size": expected_size}

    result = object_storage_service.get_file_size(path=path)

    assert result == expected_size
    object_storage_service.local_client.info.assert_called_once_with(path)


def test_get_file_info_local(object_storage_service: ObjectStorageService) -> None:
    path = "/tmp/test/file.txt"
    expected_info = {
        "size": 2048,
        "mtime": 1234567890,
        "type": "file",
    }
    object_storage_service.local_client.info.return_value = expected_info

    result = object_storage_service.get_file_info(path=path)

    assert result == expected_info
    object_storage_service.local_client.info.assert_called_once_with(path)


def test_copy_file_local_to_local(
    object_storage_service: ObjectStorageService,
) -> None:
    source_path = "/tmp/source/file.txt"
    dest_path = "/tmp/dest/file.txt"

    object_storage_service.copy_file(source_path=source_path, dest_path=dest_path)

    object_storage_service.local_client.copy.assert_called_once_with(
        source_path, dest_path
    )


def test_move_file_local_to_local(
    object_storage_service: ObjectStorageService,
) -> None:
    source_path = "/tmp/source/file.txt"
    dest_path = "/tmp/dest/file.txt"

    object_storage_service.move_file(source_path=source_path, dest_path=dest_path)

    object_storage_service.local_client.move.assert_called_once_with(
        source_path, dest_path
    )
