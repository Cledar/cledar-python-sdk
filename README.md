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
