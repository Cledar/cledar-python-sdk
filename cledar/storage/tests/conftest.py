import pytest
from faker import Faker

from cledar.storage import ObjectStorageServiceConfig

fake = Faker()


@pytest.fixture
def object_storage_config() -> ObjectStorageServiceConfig:
    return ObjectStorageServiceConfig(
        s3_access_key=fake.password(),
        s3_endpoint_url=fake.url(),
        s3_secret_key=fake.password(),
        s3_max_concurrency=10,
        azure_account_name=fake.word(),
        azure_account_key=fake.password(),
    )
