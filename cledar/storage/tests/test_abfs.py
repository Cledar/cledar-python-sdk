# mypy: disable-error-code=no-untyped-def
# pylint: disable=import-error
import io
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker

from cledar.storage import ObjectStorageService, ObjectStorageServiceConfig

fake = Faker()


@pytest.fixture(name="object_storage_service")
@patch("fsspec.filesystem")
def fixture_object_storage_service(
    fsspec_client: MagicMock, object_storage_config: ObjectStorageServiceConfig
) -> ObjectStorageService:
    # first call returns s3, second file, third abfs
    fsspec_client.side_effect = [MagicMock(), MagicMock(), MagicMock()]
    return ObjectStorageService(object_storage_config)


def test_upload_file_to_abfs_path(object_storage_service: ObjectStorageService) -> None:
    file_path = fake.file_path()
    dest_path = "abfs://container/path/to/file.txt"
    object_storage_service.azure_client.put = MagicMock()

    object_storage_service.upload_file(file_path=file_path, destination_path=dest_path)

    object_storage_service.azure_client.put.assert_called_once_with(
        lpath=file_path, rpath=dest_path
    )


def test_upload_buffer_to_abfs_path(
    object_storage_service: ObjectStorageService,
) -> None:
    buffer_bytes = io.BytesIO(fake.text().encode())
    dest_path = "abfss://container/path/to/file.txt"

    mock_file = MagicMock()
    mock_file.write = MagicMock()

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    object_storage_service.azure_client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    object_storage_service.upload_buffer(
        buffer=buffer_bytes, destination_path=dest_path
    )

    object_storage_service.azure_client.open.assert_called_once_with(
        path=dest_path, mode="wb"
    )
    mock_file.write.assert_called_once()


def test_read_file_from_abfs_path(object_storage_service: ObjectStorageService) -> None:
    path = "abfs://container/path/to/file.txt"
    expected_content = fake.text().encode()

    mock_file = MagicMock()
    mock_file.read.return_value = expected_content

    @contextmanager
    def open_cm(*_args: object, **_kwargs: object):
        yield mock_file

    object_storage_service.azure_client.open = MagicMock(
        side_effect=lambda *a, **k: open_cm()
    )

    result = object_storage_service.read_file(path=path)
    assert result == expected_content
    object_storage_service.azure_client.open.assert_called_once_with(
        path=path, mode="rb"
    )


def test_list_objects_abfs_recursive(
    object_storage_service: ObjectStorageService,
) -> None:
    path = "abfs://container/prefix"
    mock_objects = [
        "abfs://container/prefix/file1",
        "abfs://container/prefix/file2",
    ]
    object_storage_service.azure_client.find.return_value = mock_objects

    result = object_storage_service.list_objects(path=path, recursive=True)
    assert result == mock_objects
    object_storage_service.azure_client.find.assert_called_once_with(path)


def test_list_objects_abfs_non_recursive(
    object_storage_service: ObjectStorageService,
) -> None:
    path = "abfs://container/prefix"
    mock_objects = ["abfs://container/prefix/file1"]
    object_storage_service.azure_client.ls.return_value = mock_objects

    result = object_storage_service.list_objects(path=path, recursive=False)
    assert result == mock_objects
    object_storage_service.azure_client.ls.assert_called_once_with(path, detail=False)


def test_delete_file_abfs(object_storage_service: ObjectStorageService) -> None:
    path = "abfs://container/file.txt"
    object_storage_service.delete_file(path=path)
    object_storage_service.azure_client.rm.assert_called_once_with(path)


def test_file_exists_abfs(object_storage_service: ObjectStorageService) -> None:
    path = "abfs://container/file.txt"
    object_storage_service.azure_client.exists.return_value = True
    assert object_storage_service.file_exists(path=path) is True
    object_storage_service.azure_client.exists.assert_called_once_with(path)


def test_download_file_from_abfs(object_storage_service: ObjectStorageService) -> None:
    source_path = "abfs://container/file.txt"
    dest_path = "/tmp/dest.txt"
    object_storage_service.azure_client.get = MagicMock()
    object_storage_service.download_file(dest_path, source_path=source_path)
    object_storage_service.azure_client.get.assert_called_once_with(
        source_path, dest_path
    )


def test_get_file_size_abfs(object_storage_service: ObjectStorageService) -> None:
    path = "abfs://container/file.txt"
    object_storage_service.azure_client.info.return_value = {"size": 123}
    result = object_storage_service.get_file_size(path=path)
    assert result == 123


def test_get_file_info_abfs(object_storage_service: ObjectStorageService) -> None:
    path = "abfs://container/file.txt"
    info = {"size": 1}
    object_storage_service.azure_client.info.return_value = info
    result = object_storage_service.get_file_info(path=path)
    assert result == info


def test_copy_file_abfs_to_abfs(object_storage_service: ObjectStorageService) -> None:
    src = "abfs://container/src.txt"
    dst = "abfs://container/dst.txt"
    object_storage_service.azure_client.copy = MagicMock()
    object_storage_service.copy_file(source_path=src, dest_path=dst)
    object_storage_service.azure_client.copy.assert_called_once_with(src, dst)


def test_move_file_abfs_to_abfs(object_storage_service: ObjectStorageService) -> None:
    src = "abfs://container/src.txt"
    dst = "abfs://container/dst.txt"
    object_storage_service.azure_client.move = MagicMock()
    object_storage_service.move_file(source_path=src, dest_path=dst)
    object_storage_service.azure_client.move.assert_called_once_with(src, dst)
