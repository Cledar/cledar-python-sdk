import pytest
from faker import Faker
from storage_service.object_storage import ObjectStorageServiceConfig

fake = Faker()


@pytest.fixture
def object_storage_config() -> ObjectStorageServiceConfig:
    return ObjectStorageServiceConfig(
        s3_access_key=fake.password(),
        s3_endpoint_url=fake.url(),
        s3_secret_key=fake.password(),
    )
