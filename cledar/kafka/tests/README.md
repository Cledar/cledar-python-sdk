# Kafka Service Tests

This directory contains the test suite for the Kafka service, organized into unit and integration tests.

## Directory Structure

```
tests/
├── conftest.py              # Test-wide teardown (cleans Kafka client threads)
├── README.md
├── unit/                    # Unit tests (176 tests)
│   ├── test_base_kafka_client.py
│   ├── test_config_validation.py
│   ├── test_dead_letter_handler.py
│   ├── test_error_handling.py
│   ├── test_input_parser.py
│   ├── test_input_parser_comprehensive.py
│   ├── test_utils.py
│   ├── test_utils_comprehensive.py
│   └── requirements-test.txt
└── integration/             # Integration tests (41 tests)
    ├── conftest.py          # Shared Kafka fixtures (container, configs, clients)
    ├── helpers.py           # E2EData, consume_until, ensure_topic_and_subscribe
    ├── test_integration.py
    ├── test_producer_integration.py
    ├── test_consumer_integration.py
    └── test_producer_consumer_interaction.py
```

## Test Categories

### Unit Tests (`unit/`)
Unit tests focus on testing individual components in isolation using mocks and stubs. They are fast, reliable, and don't require external dependencies.

- **Base Client Tests**: Test the base Kafka client functionality
- **Config Validation**: Test configuration validation and schema validation
- **Dead Letter Handler**: Test dead letter queue handling with mocked producers
- **Error Handling**: Test error scenarios and exception handling
- **Input Parser**: Test message parsing and validation
- **Utils**: Test utility functions and helper methods

### Integration Tests (`integration/`)
Integration tests use real external dependencies (like Kafka via testcontainers) to test the complete flow of the system.

- **Real Kafka Integration**: Tests with actual Kafka instance using testcontainers
- **Producer Integration**: Real producer operations and message sending
- **Consumer Integration**: Real consumer operations and message consumption
- **Producer-Consumer Interaction**: Real interaction patterns between producer and consumer
- **End-to-End Flows**: Complete producer-consumer workflows
- **Connection Recovery**: Real connection failure and recovery scenarios
- **Performance Tests**: Stress tests and large message handling

## Running Tests

### Run All Tests
```bash
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/
```

### Run Only Unit Tests
```bash
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/unit/
```

### Run Only Integration Tests
```bash
# Run all integration tests
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/integration/

# Run specific integration test file
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/integration/test_producer_integration.py

# Run single test
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/integration/test_integration.py::test_end_to_end_message_flow -v
```

### Run Specific Test Files
```bash
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/unit/test_config_validation.py
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/integration/test_integration.py
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/integration/test_producer_integration.py
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/integration/test_consumer_integration.py
```

## Test Requirements

- **Unit Tests**: No external dependencies required
- **Integration Tests**: Requires Docker to be running for testcontainers
- **Slow Integration Tests**: Marked as skipped by default due to execution time (2-5 minutes each)

## Performance Notes

- **Unit Tests**: Fast execution (~10–15 seconds for all 176 tests)
- **Integration Tests**: Moderate execution (~2–2.5 minutes for 41 tests)
- Helpers reduce flakiness: `consume_until()` polls with timeout instead of fixed sleeps

## Docker Setup for Integration Tests

The integration tests use testcontainers to spin up real Kafka instances for testing. This requires Docker to be installed and running.

### Prerequisites

1. **Install Docker Desktop** (recommended):
   - [Docker Desktop for Mac](https://docs.docker.com/desktop/mac/install/)
   - [Docker Desktop for Windows](https://docs.docker.com/desktop/windows/install/)
   - [Docker Desktop for Linux](https://docs.docker.com/desktop/linux/install/)

2. **Or install Docker Engine** (alternative):
   ```bash
   # macOS (using Homebrew)
   brew install docker
   
   # Ubuntu/Debian
   sudo apt-get update
   sudo apt-get install docker.io
   
   # CentOS/RHEL
   sudo yum install docker
   ```

### Starting Docker

#### Docker Desktop
1. Launch Docker Desktop application
2. Wait for Docker to start (you'll see the Docker whale icon in your system tray)
3. Verify Docker is running:
   ```bash
   docker --version
   docker ps
   ```

#### Docker Engine (Linux)
```bash
# Start Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Add your user to docker group (optional, to avoid sudo)
sudo usermod -aG docker $USER
# Log out and back in for group changes to take effect

# Verify Docker is running
docker --version
docker ps
```

### Running Integration Tests

Once Docker is running, you can execute the integration tests:

```bash
# Run all integration tests
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/integration/

# Run a specific integration test
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/integration/test_integration.py::test_producer_consumer_basic_flow

# Run integration tests with verbose output
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/integration/ -v

# Run integration tests and show logs
PYTHONPATH=. uv run pytest cledar/kafka_service/tests/integration/ -s
```

### Troubleshooting Docker Issues

#### Docker not running
```bash
# Check if Docker is running
docker ps
# If you get "Cannot connect to the Docker daemon", Docker is not running

# Start Docker Desktop or Docker service
# Docker Desktop: Launch the application
# Docker Engine: sudo systemctl start docker
```

#### Permission denied errors
```bash
# Add user to docker group (Linux)
sudo usermod -aG docker $USER
# Log out and back in

# Or run with sudo (not recommended)
sudo docker ps
```

#### Port conflicts
If you have Kafka running locally on port 9092, the testcontainers will automatically use different ports. No action needed.

#### Resource constraints
If tests fail due to memory/CPU constraints:
```bash
# Check Docker resource limits in Docker Desktop settings
# Increase memory allocation if needed (recommended: 4GB+)
```

### Test Container Details

The integration tests use:
- **Kafka Image**: `confluentinc/cp-kafka:7.4.0`
- **Automatic Port Assignment**: testcontainers handles port conflicts
- **Automatic Cleanup**: containers are removed after tests complete
- **Session Scope**: Kafka container is shared across all integration tests in a session

## Test Statistics

- **Total Tests**: 217
- **Unit Tests**: 176
- **Integration Tests**: 41

## Notes

- All tests use `PYTHONPATH=.` to ensure proper module imports
- Integration tests use shared fixtures in `integration/conftest.py` and helpers in `integration/helpers.py`
- Test-wide teardown in `tests/conftest.py` ensures Kafka client threads don’t block process exit
