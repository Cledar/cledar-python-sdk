# Universal Formatter

The `UniversalPlaintextFormatter` is a custom logging formatter that extends the standard `logging.Formatter` class. It adds the ability to include extra attributes from log records while excluding standard attributes and configurable keys.

## Usage

To use the `UniversalPlaintextFormatter` in your logging configuration, add the following to your `logging.conf` file:

```ini
[formatter_plaintextFormatter]
class=questions_generator.common_services.logging.universal_formatter.UniversalPlaintextFormatter
format=%(asctime)s %(name)s [%(levelname)s]: %(message)s
datefmt=%Y-%m-%d %H:%M:%S
```

## Features

- Extends the standard logging.Formatter
- Automatically includes extra attributes from log records
- Excludes standard LogRecord attributes to keep logs clean
- Configurable exclusion of additional keys

## Configuration Options

In addition to the standard formatter options, you can configure which keys to exclude from the log output:

```ini
[formatter_plaintextFormatter]
class=questions_generator.common_services.logging.universal_formatter.UniversalPlaintextFormatter
format=%(asctime)s %(name)s [%(levelname)s]: %(message)s
datefmt=%Y-%m-%d %H:%M:%S
exclude_keys=key1,key2,key3
```

The `exclude_keys` option allows you to specify a comma-separated list of keys that should be excluded from the log output, in addition to the standard LogRecord attributes.

## Example

When using this formatter, any extra attributes added to the log record will be automatically included in the log output:

```python
import logging

logger = logging.getLogger(__name__)
logger.info("User logged in", extra={"user_id": 123, "ip_address": "192.168.1.1"})
```

Output:
```
2023-08-04 12:34:56 my_module [INFO]: User logged in
    user_id: 123
    ip_address: 192.168.1.1
```