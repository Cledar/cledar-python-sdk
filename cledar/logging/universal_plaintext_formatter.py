import configparser
import logging
from typing import Any


class UniversalPlaintextFormatter(logging.Formatter):
    """
    A custom formatter for logging that extends the standard logging.Formatter.

    This formatter adds the ability to include extra attributes from log records while
    excluding standard attributes and configurable keys.
    """

    # Predefined exclusions - keys that should always be excluded
    DEFAULT_EXCLUDE_KEYS = {"message", "asctime"}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the formatter with standard formatter parameters.

        Args:
            *args: Variable length argument list for the parent class.
            **kwargs: Arbitrary keyword arguments for the parent class.
        """
        super().__init__(*args, **kwargs)
        self._standard_attrs: set[str] | None = None
        self._config_exclude_keys = self._load_exclude_keys_from_config()

    def _load_exclude_keys_from_config(self) -> set[str]:
        """
        Load additional keys to exclude from the configuration file.

        Returns:
            set: A set of keys to exclude from log records.
        """
        try:
            config = configparser.ConfigParser()
            config.read("logging.conf")
            if config.has_option("formatter_plaintextFormatter", "exclude_keys"):
                exclude_str = config.get("formatter_plaintextFormatter", "exclude_keys")
                return set(key.strip() for key in exclude_str.split(",") if key.strip())
        except (configparser.Error, FileNotFoundError, PermissionError, ValueError):
            pass
        return set()

    def _get_standard_attrs(self) -> set[str]:
        """
        Get the set of standard attributes to exclude from log records.

        This includes standard LogRecord attributes, predefined exclusions,
        and exclusions from configuration.

        Returns:
            set: A set of attribute names to exclude.
        """
        if self._standard_attrs is None:
            dummy_record = logging.LogRecord(
                name="dummy",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="",
                args=(),
                exc_info=None,
            )
            # Combine standard attributes + predefined + from configuration
            all_excludes = (
                set(dummy_record.__dict__.keys())
                | self.DEFAULT_EXCLUDE_KEYS
                | self._config_exclude_keys
            )
            self._standard_attrs = all_excludes
        return self._standard_attrs

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record, adding any extra attributes not in the standard set.

        Args:
            record: The log record to format.

        Returns:
            str: The formatted log message with extra attributes appended.
        """
        base = super().format(record)
        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k not in self._get_standard_attrs()
        }
        if extras:
            extras_str = "\n".join(f"    {k}: {v}" for k, v in extras.items())
            return f"{base}\n{extras_str}"
        return base
