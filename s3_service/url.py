import dataclasses


@dataclasses.dataclass
class S3Metadata:
    bucket: str
    key: str


class IncorrectSchemaException(Exception):
    """Url needs to start with `s3://`"""


def parse_url(url: str) -> S3Metadata:
    if not url.startswith("s3://"):
        raise IncorrectSchemaException

    just_data = url.replace("s3://", "")
    bucket, key = just_data.split("/", maxsplit=1)

    return S3Metadata(
        bucket=bucket,
        key=key,
    )
