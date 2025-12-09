# Storage Service

## Purpose

The `cledar.storage` package provides a unified interface for interacting with S3-compatible object storage (like AWS S3, MinIO), Azure Blob Storage via ABFS/ABFSS (adlfs), and local filesystem storage. It abstracts away the complexity of managing files across different storage backends, providing a consistent API for common operations.

### Key Features

- **Unified API**: Single interface for S3, Azure ABFS, and local filesystem operations
- **S3 Compatible**: Works with AWS S3, MinIO, and other S3-compatible storage systems
- **Azure ABFS Support**: Works with Azure Blob Storage using `abfs://` or `abfss://` URIs (via `adlfs`)
- **Comprehensive Operations**: Upload, download, list, copy, move, and delete files
- **Metadata Support**: Get file size, info, and check existence
- **Buffer Support**: Upload/download directly from/to memory buffers
- **Retry Logic**: Built-in retry mechanisms for network operations
- **Type Safety**: Fully typed with Python type hints
- **Well Tested**: Extensive unit tests and integration tests

### Use Cases

- Storing and retrieving application data from S3
- Managing media files (images, videos, audio)
- Handling temporary file storage
- Cross-platform file operations (local and cloud)
- Backup and archival systems

## Installation

This package is part of the Cledar SDK. Install it using:

```bash
# Install with uv (recommended)
uv sync --all-groups

# Or with pip
pip install -e .
```

## Usage Example

```python
from cledar.storage import ObjectStorageService, ObjectStorageServiceConfig
import io

# Configure the service
config = ObjectStorageServiceConfig(
    s3_endpoint_url="https://s3.amazonaws.com",
    s3_access_key="your-access-key",
    s3_secret_key="your-secret-key",
    s3_max_concurrency=10,
)

# Create service instance
service = ObjectStorageService(config)

# Upload a file from buffer
buffer = io.BytesIO(b"Hello, World!")
service.upload_buffer(buffer=buffer, bucket="my-bucket", key="hello.txt")

# Read a file
content = service.read_file(bucket="my-bucket", key="hello.txt")
print(content)  # b"Hello, World!"

# List objects
files = service.list_objects(bucket="my-bucket", prefix="folder/", recursive=True)
print(files)  # ['folder/file1.txt', 'folder/file2.txt', ...]

# Check if file exists
exists = service.file_exists(bucket="my-bucket", key="hello.txt")
print(exists)  # True

# Get file metadata
size = service.get_file_size(bucket="my-bucket", key="hello.txt")
info = service.get_file_info(bucket="my-bucket", key="hello.txt")

# Copy file
service.copy_file(
    source_bucket="my-bucket",
    source_key="hello.txt",
    dest_bucket="my-bucket",
    dest_key="hello-copy.txt"
)

# Delete file
service.delete_file(bucket="my-bucket", key="hello.txt")
```

## Development

### Project Structure

```
cledar/storage/
├── __init__.py              # Package initialization
├── exceptions.py            # Custom exceptions
├── object_storage.py        # Main service implementation
├── tests/
│   ├── conftest.py         # Pytest fixtures
│   ├── test_s3.py          # Unit tests for S3 operations
│   ├── test_local.py       # Unit tests for local operations
│   └── test_integration_s3.py  # Integration tests with real MinIO
└── README.md               # This file
```

## Running Linters

The project is configured for multiple linters (see `pyproject.toml` for configuration).

### Available Linter Configurations

The project includes configurations for:
- **Pylint**: Python code analysis (`.tool.pylint` in `pyproject.toml`)
- **Mypy**: Static type checking (`.tool.mypy` in `pyproject.toml`)
- **Black**: Code formatting (`.tool.black` in `pyproject.toml`)

### Installing Linters

Linters are not included in the dev dependencies by default. Install them separately:

```bash
# Install all linters
pip install pylint mypy black

# Or with uv
uv pip install pylint mypy black
```

### Running Linters

Once installed, run them from the SDK root directory:

```bash
# From the SDK root directory
cd /path/to/cledar-python-sdk

# Run pylint on storage
pylint cledar/storage/

# Run mypy type checking (strict mode configured)
mypy cledar/storage/

# Check code formatting with black
black --check cledar/storage/

# Auto-format code
black cledar/storage/
```

### Run All Linters

```bash
# Run all linters in sequence
pylint cledar/storage/ && \
mypy cledar/storage/ && \
black --check cledar/storage/
```

### IDE Integration

Most IDEs support these linters natively:
- **VSCode**: Install Python extension, linters auto-detected via `pyproject.toml`
- **PyCharm**: Enable in Settings → Tools → Python Integrated Tools
- **Cursor**: Same as VSCode

## Running Unit Tests

Unit tests use mocks to test the code in isolation without requiring external dependencies.

### Run All Unit Tests

```bash
# From the SDK root directory
cd /path/to/cledar-python-sdk

# Set PYTHONPATH and run unit tests
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_s3.py cledar/storage/tests/test_local.py -v
```

### Run Specific Test File

```bash
# Run S3 unit tests
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_s3.py -v

# Run local filesystem unit tests
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_local.py -v
```

### Run Specific Test

```bash
# Run a specific test by name
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_s3.py::test_upload_file_filesystem_with_bucket_key_should_use_s3 -v
```

### Run with Coverage

```bash
# Generate coverage report
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_s3.py cledar/storage/tests/test_local.py \
  --cov=storage_service \
  --cov-report=html \
  --cov-report=term

# View HTML report
open htmlcov/index.html
```

### Unit Test Details

- **Test Framework**: pytest
- **Mocking**: unittest.mock
- **Fixtures**: Defined in `conftest.py`
- **Test Count**: 54 unit tests
- **Execution Time**: ~0.1 seconds (fast, no external dependencies)

#### What Unit Tests Cover:

- ✅ S3 operations (upload, download, list, delete, copy, move)
- ✅ Local filesystem operations
- ✅ Error handling and exceptions
- ✅ Parameter validation
- ✅ Retry mechanisms
- ✅ Buffer operations
- ✅ Metadata operations

## Running Integration Tests

Integration tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to spin up a real MinIO container and test against actual S3-compatible storage.

### Prerequisites

**Required**:
- Docker installed and running on your machine
- Network access to pull Docker images

**Optional**:
- Docker Desktop (macOS/Windows) or Docker Engine (Linux)

### Run All Integration Tests

```bash
# From the SDK root directory
cd /path/to/cledar-python-sdk

# Set PYTHONPATH and run integration tests
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_integration_s3.py -v
```

### Run Specific Integration Test Class

```bash
# Run only basic operations tests
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_integration_s3.py::TestIntegrationBasicOperations -v

# Run only file operations tests
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_integration_s3.py::TestIntegrationFileOperations -v
```

### Run with Detailed Output

```bash
# Show container startup logs
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_integration_s3.py -v -s --log-cli-level=INFO
```

### Integration Test Details

- **Test Framework**: pytest + testcontainers
- **Container**: MinIO (S3-compatible storage)
- **Image**: `minio/minio:RELEASE.2022-12-02T19-19-22Z`
- **Test Count**: 21 integration tests
- **Execution Time**: ~3 seconds (includes container startup)

#### How Integration Tests Work

1. **Container Startup** (Module Scope):
   ```python
   @pytest.fixture(scope="module")
   def minio_container():
       """Start a MinIO container for testing."""
       with MinioContainer(
           access_key="minioadmin",
           secret_key="minioadmin",
       ) as minio:
           yield minio
   ```
   - Container starts once for all tests in the module
   - Automatically pulls MinIO Docker image if not present
   - Exposes MinIO on a random available port

2. **Service Configuration** (Module Scope):
   ```python
   @pytest.fixture(scope="module")
   def object_storage_service(minio_container):
       """Create an ObjectStorageService connected to MinIO."""
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
   ```
   - Creates service instance connected to MinIO container
   - Shared across all tests in the module

3. **Test Isolation** (Function Scope):
   ```python
   @pytest.fixture
   def test_bucket(object_storage_service):
       """Create a unique test bucket for each test."""
       bucket_name = f"test-bucket-{fake.uuid4()}"
       object_storage_service.client.mkdir(f"s3://{bucket_name}")
       yield bucket_name
       # Cleanup: delete all objects and bucket
       try:
           objects = object_storage_service.list_objects(
               bucket=bucket_name, recursive=True
           )
           for obj in objects:
               object_storage_service.delete_file(bucket=bucket_name, key=obj)
           object_storage_service.client.rmdir(f"s3://{bucket_name}")
       except Exception:
           pass
   ```
   - Each test gets a unique bucket (UUID-based name)
   - Bucket and all its contents are cleaned up after each test
   - Ensures complete test isolation

4. **Automatic Cleanup**:
   - testcontainers automatically stops and removes containers after tests
   - No manual cleanup required
   - Containers are cleaned up even if tests fail

#### What Integration Tests Cover:

- ✅ **Basic Operations** (4 tests): Connection health, bucket existence, error handling
- ✅ **Buffer Operations** (2 tests): Upload/read buffers, parameter validation
- ✅ **File Operations** (2 tests): Upload/download files, retry mechanisms
- ✅ **List Operations** (3 tests): Recursive/non-recursive listing, prefix filtering
- ✅ **File Management** (4 tests): Existence checks, deletion, metadata retrieval
- ✅ **Copy/Move Operations** (2 tests): File copying and moving
- ✅ **Error Handling** (3 tests): Nonexistent file operations, invalid parameters
- ✅ **Large Files** (1 test): 10MB file upload/download

### Run All Tests (Unit + Integration)

```bash
# Run everything
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/ -v

# Run with coverage
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/ \
  --cov=storage_service \
  --cov-report=html \
  --cov-report=term \
  -v
```

**Total Test Count**: 75 tests (54 unit + 21 integration)


#### Slow First Run
First execution is slower due to:
- Pulling testcontainers/ryuk image
- Pulling MinIO image
- Container initialization

Subsequent runs are much faster (~2.5s).


## CI/CD Integration

### GitLab CI Example

```yaml
test-unit:
  stage: test
  image: python:3.12
  script:
    - pip install uv
    - uv sync --all-groups
    - PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_s3.py cledar/storage/tests/test_local.py -v

test-integration:
  stage: test
  image: python:3.12
  services:
    - docker:dind  # Docker-in-Docker for testcontainers
  variables:
    DOCKER_HOST: tcp://docker:2375
    DOCKER_TLS_CERTDIR: ""
  script:
    - pip install uv
    - uv sync --all-groups
    - PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_integration_s3.py -v
```

### GitHub Actions Example

```yaml
name: Tests
on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.12'
      - name: Install dependencies
        run: |
          pip install uv
          uv sync --all-groups
      - name: Run unit tests
        run: PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_s3.py cledar/storage/tests/test_local.py -v

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.12'
      - name: Install dependencies
        run: |
          pip install uv
          uv sync --all-groups
      - name: Run integration tests
        run: PYTHONPATH=$PWD uv run pytest cledar/storage/tests/test_integration_s3.py -v
```

## API Reference

### ObjectStorageServiceConfig

Configuration dataclass for the storage service.

```python
@dataclass
class ObjectStorageServiceConfig:
    s3_endpoint_url: str      # S3 endpoint URL
    s3_access_key: str        # Access key
    s3_secret_key: str        # Secret key
    s3_max_concurrency: int   # Max concurrent connections
```

### ObjectStorageService

Main service class providing storage operations.

#### Methods

- `is_alive() -> bool` - Check if service can connect to storage
- `has_bucket(bucket: str, throw: bool = False) -> bool` - Check if bucket exists
- `upload_buffer(buffer, bucket, key)` - Upload from memory buffer
- `upload_file(file_path, bucket, key)` - Upload from file
- `read_file(bucket, key, max_tries=3) -> bytes` - Read file contents
- `download_file(dest_path, bucket, key, max_tries=3)` - Download to file
- `list_objects(bucket, prefix="", recursive=True) -> list[str]` - List objects
- `delete_file(bucket, key)` - Delete a file
- `file_exists(bucket, key) -> bool` - Check if file exists
- `get_file_size(bucket, key) -> int` - Get file size in bytes
- `get_file_info(bucket, key) -> dict` - Get file metadata
- `copy_file(source_bucket, source_key, dest_bucket, dest_key)` - Copy file
- `move_file(source_bucket, source_key, dest_bucket, dest_key)` - Move file

All methods support both S3 operations (using `bucket` and `key`), Azure ABFS operations (using `path` starting with `abfs://` or `abfss://`), and local filesystem operations (using `path` or `destination_path`). Mixed-backend copy/move (e.g., S3 to ABFS) is supported via streamed transfer.

### Azure Configuration

Install the optional dependency:

```bash
uv sync --all-groups  # or pip install adlfs
```

Provide credentials through `ObjectStorageServiceConfig` (any that apply):

```python
config = ObjectStorageServiceConfig(
    s3_endpoint_url="https://s3.amazonaws.com",
    s3_access_key="...",
    s3_secret_key="...",
    s3_max_concurrency=10,
    # Azure optional settings
    azure_account_name="youraccount",           # optional
    azure_account_key="<account key>",          # or OAuth below
    azure_tenant_id="<tenant id>",              # optional OAuth
    azure_client_id="<client id>",              # optional OAuth
    azure_client_secret="<client secret>",      # optional OAuth
)

service = ObjectStorageService(config)
content = service.read_file(path="abfs://container/path/to/file.txt")
```


### Running Pre-commit Checks

```bash
# Format code
uv run black cledar/storage/

# Check types
uv run mypy cledar/storage/

# Run linter
uv run pylint cledar/storage/

# Run all tests
PYTHONPATH=$PWD uv run pytest cledar/storage/tests/ -v
```

## License

See the main repository LICENSE file.

## Support

For issues, questions, or contributions, please refer to the main repository's contribution guidelines.

