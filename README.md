# Cledar Python SDK

## Project Description

**Cledar Python SDK** is a git submodule that contains shared services (S3, kafka service, dlq service, monitoring service) redy for use across projects. It is used by multiple workers to ensure consistency and compatibility between different components.
---

## Installation and Setup

1. **Clone the Repository**
   ```bash
   git clone git@github.com:Cledar/cledar-python-sdk.git
   ```

2. **Install Dependencies**
   ```bash
   uv sync
   ```
3. **How to use this repo in your project**
   To use this repo in your project, you can add it as a submodule like this:
   ```bash
   git submodule add <submodule_url> [optional_path]
   ```
   Then you can import the services in your project like this:
   ```python
   from common_services.kafka_service.kafka_producer import KafkaProducer

   ```
   etc.

## Testing

Unit tests are implemented using **pytest** and **unittest**.

1. Run tests:
   ```bash
   uv run pytest
   ```

2. Adding tests:
   Place your tests in the *_service/tests folder or as files with the _test.py suffix in */tests folder.

## Code Quality

- **pydantic** - settings management
- **ruff**, **mypy** - Linting, formatting, and static type checking
- **pre-commit** - Pre-commit file checks

## Linting 

If you want to run linting or type checker manually, you can use the following commands. Pre-commit will run these checks automatically before each commit.
```bash
uv run ruff format .
uv run ruff check .
uv run mypy .
```

## Pre-commit setup

To get started follow these steps:

1. Install `pre-commit` by running the following command:
    ```
    pip install pre-commit
    ```

2. Once `pre-commit` is installed, set up the pre-commit hooks by running:
    ```
    pre-commit install
    ```

3. Pre-commit hooks will analyze only commited files. To analyze all files after installation run the following:
    ```
    pre-commit run --all-files
    ```


### Automatic Fixing Before Commits:
pre-commit will run Ruff (format + lint) and mypy during the commit process:

   ```bash
   git commit -m "Describe your changes"
   ```
To skip pre-commit hooks for a single commit, use the `--no-verify` flag:

    ```bash
    git commit -m "Your commit message" --no-verify
    ```

---

## Technologies and Libraries

### Main Dependencies:
 - **python** = "3.12.7"
 - **pydantic-settings** = "2.3.3"
 - **confluent-kafka** = "2.4.0"
 - **fastapi** = "^0.112.3"
 - **prometheus-client** = "^0.20.0"
 - **uvicorn** = "^0.30.6"


### Developer Tools:
- **uv** - Dependency and environment management
- **pydantic** - settings management
- **ruff** - Linting and formatting
- **mypy** - Static type checker
- **pytest**, **unittest** - Unit tests
- **pre-commit** - Code quality hooks

---

## Commit conventions

We use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for our commit messages. This helps us to create a better, more readable changelog.

Example of a commit message:
```bash
refactor(XXX-NNN): spaghetti code is now a carbonara
```
