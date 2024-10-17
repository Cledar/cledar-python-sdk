import pytest
from faker import Faker
from s3_service.s3 import S3ServiceConfig

fake = Faker()


@pytest.fixture
def s3_config() -> S3ServiceConfig:
    return S3ServiceConfig(
        s3_access_key=fake.password(),
        s3_endpoint_url=fake.url(),
        s3_secret_key=fake.password(),
    )
