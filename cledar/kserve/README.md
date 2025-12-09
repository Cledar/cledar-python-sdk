# KServe Service

## Purpose

The `cledar.kserve` package provides utilities for working with KServe inference services, particularly for handling CloudEvents headers that are used in KServe's event-driven architecture. It simplifies the extraction and parsing of metadata from CloudEvents headers, making it easier to integrate with KServe deployments.

### Key Features

- **CloudEvents Parsing**: Extract Kafka topic names from CloudEvents source headers
- **Header Validation**: Robust validation of CloudEvents header format
- **Type Safety**: Fully typed with Python type hints
- **Well Tested**: Comprehensive unit tests covering edge cases
- **Lightweight**: Minimal dependencies, focused utility functions

### Use Cases

- Parsing CloudEvents headers in KServe inference services
- Extracting Kafka topic information from event-driven requests
- Building event-driven ML inference pipelines
- Integration with KServe and Knative Eventing

## Installation

This package is part of the `cledar-python-sdk`. Install it using:

```bash
# Install with uv (recommended)
uv sync --all-groups

# Or with pip
pip install -e .
```

## Usage Example

```python
from cledar.kserve import get_input_topic

# Example CloudEvents headers from KServe request
headers = {
    "ce-source": "kafka://my-cluster#input-topic",
    "ce-type": "dev.knative.kafka.event",
    "ce-id": "partition:0/offset:123",
}

# Extract the Kafka topic name
topic = get_input_topic(headers)
print(topic)  # Output: "input-topic"

# Handle missing or invalid headers
empty_headers = {}
topic = get_input_topic(empty_headers)
print(topic)  # Output: None

# Handle headers without delimiter
invalid_headers = {"ce-source": "kafka://my-cluster/topic"}
topic = get_input_topic(invalid_headers)
print(topic)  # Output: None
```

## Development

### Project Structure

```
cledar/kserve/
├── __init__.py              # Package initialization with exports
├── utils.py                 # Utility functions for CloudEvents
├── tests/
│   ├── __init__.py         # Test package initialization
│   └── test_utils.py       # Unit tests for utilities
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

# Run pylint on cledar.kserve
pylint cledar/kserve/

# Run mypy type checking (strict mode configured)
mypy cledar/kserve/

# Check code formatting with black
black --check cledar/kserve/

# Auto-format code
black cledar/kserve/
```

### Run All Linters

```bash
# Run all linters in sequence
pylint cledar/kserve/ && \
mypy cledar/kserve/ && \
black --check cledar/kserve/
```

### IDE Integration

Most IDEs support these linters natively:
- **VSCode**: Install Python extension, linters auto-detected via `pyproject.toml`
- **PyCharm**: Enable in Settings → Tools → Python Integrated Tools
- **Cursor**: Same as VSCode

## Running Unit Tests

Unit tests verify the functionality of the CloudEvents parsing utilities.

### Run All Unit Tests

```bash
# From the SDK root directory
cd /path/to/cledar-python-sdk

# Run all tests using uv
PYTHONPATH=$PWD uv run pytest cledar/kserve/tests/ -v
```

### Run Specific Test File

```bash
# Run specific test file
PYTHONPATH=$PWD uv run pytest cledar/kserve/tests/test_utils.py -v
```

### Run Specific Test

```bash
# Run a specific test by name
PYTHONPATH=$PWD uv run pytest cledar/kserve/tests/test_utils.py::test_get_input_topic_valid_source -v
```

### Run with Coverage

```bash
# Generate coverage report
PYTHONPATH=$PWD uv run pytest cledar/kserve/tests/ \
  --cov=cledar.kserve \
  --cov-report=html \
  --cov-report=term

# View HTML report
open htmlcov/index.html
```

### Unit Test Details

- **Test Framework**: pytest
- **Test Count**: 9 unit tests
- **Execution Time**: ~0.04 seconds (fast, no external dependencies)

#### What Unit Tests Cover:

- ✅ Valid CloudEvents source parsing
- ✅ Whitespace trimming and normalization
- ✅ Missing header handling
- ✅ Invalid format detection (no delimiter)
- ✅ Empty topic after delimiter
- ✅ Whitespace-only topics
- ✅ Multiple delimiters in source
- ✅ Empty source values
- ✅ Complex topic names with namespaces

## CI/CD Integration

### GitLab CI Example

```yaml
test-kserve-service:
  stage: test
  image: python:3.12
  script:
    - pip install uv
    - uv sync --all-groups
    - PYTHONPATH=$PWD uv run pytest cledar/kserve/tests/ -v
```

### GitHub Actions Example

```yaml
name: KServe Service Tests
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
        run: PYTHONPATH=$PWD uv run pytest cledar/kserve/tests/ -v
```

## API Reference

### Constants

#### `CE_SOURCE_HEADER`

The CloudEvents source header key used in KServe requests.

```python
CE_SOURCE_HEADER = "ce-source"
```

### Functions

#### `get_input_topic(headers: dict[str, str]) -> str | None`

Extract the Kafka topic name from CloudEvents source header.

Parses the 'ce-source' header value which is expected to be in the format `prefix#topic_name` and returns the topic name after the '#' delimiter.

**Parameters:**
- `headers` (dict[str, str]): Dictionary of HTTP headers containing CloudEvents metadata.

**Returns:**
- `str | None`: The extracted topic name if the header exists, contains '#', and has a non-empty topic name after the delimiter. Returns `None` otherwise.

**Example:**

```python
>>> headers = {"ce-source": "kafka://cluster#my-topic"}
>>> get_input_topic(headers)
'my-topic'

>>> headers = {"ce-source": "kafka://cluster#"}
>>> get_input_topic(headers)
None

>>> headers = {}
>>> get_input_topic(headers)
None
```

**Edge Cases Handled:**
- Missing `ce-source` header → Returns `None`
- No `#` delimiter in source → Returns `None`
- Empty topic after `#` → Returns `None`
- Whitespace-only topic → Returns `None` (after stripping)
- Leading/trailing whitespace → Stripped automatically
- Multiple `#` delimiters → Only first `#` is used as delimiter

## CloudEvents Format

The `ce-source` header in KServe follows the CloudEvents specification and typically has this format:

```
<protocol>://<cluster-or-namespace>#<topic-name>
```

**Examples:**
- `kafka://prod-cluster#user-events`
- `kafka://namespace.kafka#model-predictions`
- `kafka://local#ml-inference-requests`

The `get_input_topic` function extracts the `<topic-name>` portion after the `#` delimiter.

## Integration with KServe

### Example KServe Predictor

```python
from kserve import Model, ModelServer
from cledar.kserve import get_input_topic
import logging

logger = logging.getLogger(__name__)

class MyPredictor(Model):
    def __init__(self, name: str):
        super().__init__(name)
        
    def predict(self, request: dict, headers: dict[str, str]) -> dict:
        # Extract source topic from CloudEvents headers
        source_topic = get_input_topic(headers)
        
        if source_topic:
            logger.info(f"Processing request from topic: {source_topic}")
        else:
            logger.warning("Could not determine source topic from headers")
        
        # Your inference logic here
        predictions = self.model.predict(request["instances"])
        
        return {"predictions": predictions}

if __name__ == "__main__":
    model = MyPredictor("my-model")
    ModelServer().start([model])
```

## Running Pre-commit Checks

```bash
# Format code
uv run black cledar/kserve/

# Check types
uv run mypy cledar/kserve/

# Run linter
uv run pylint cledar/kserve/

# Run all tests
PYTHONPATH=$PWD uv run pytest cledar/kserve/tests/ -v
```

## License

See the main repository LICENSE file.

## Support

For issues, questions, or contributions, please refer to the main repository's contribution guidelines.

