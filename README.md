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

# Common Services - Python + Kafka

## Project Description

**Common Services** is a git submodule that contains shared services (S3, kafka service, dlq service, monitoring service) for the SR project. It is used by multiple workers to ensure consistency and compatibility between different components.
---

## Installation and Setup

1. **Clone the Repository**
   ```bash
   git clone git@gitlab.com:cledar/kim/signals/pipeline/common_services.git
   ```

2. **Install Dependencies**
   ```bash
   poetry install
   ```

## Testing

Unit tests are implemented using **pytest** and **unittest**.

1. Run tests:
   ```bash
   poetry run pytest
   ```

2. Adding tests:
   Place your tests in the *_service/tests folder or as files with the _test.py suffix in */tests folder.

## Code Quality

- **pydantic** - settings management
- **pylint**, **mypy** - Static code analysis
- **Black** - Code formatting
- **pre-commit** - Pre-commit file checks

## Linting 

```
black .
pylint $(git ls-files *.py)
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
4. To skip pre-commit hooks for a single commit, use the `--no-verify` flag:
    ```
    git commit -m "Your commit message" --no-verify
    ```


### Automatic Fixing Before Commits:
pre-commit will run Black, pylint and mypy during the commit process:
```bash
git commit -m "Describe your changes"
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
- **poetry** - Dependency management
- **pydantic** - settings management
- **pylint** - Static code analysis
- **mypy** - Static type checker
- **pytest**, **unittest** - Unit tests
- **Black** & **pre-commit** - Code quality tools

---

## Contributing

Want to contribute? Please submit a pull request!

---

## License

This project is licensed under the MIT License.
