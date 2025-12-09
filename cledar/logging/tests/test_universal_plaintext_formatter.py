# pylint: disable=unused-argument, protected-access
import logging
import os
import tempfile
from pathlib import Path

import pytest

from cledar.logging.universal_plaintext_formatter import UniversalPlaintextFormatter


@pytest.fixture(name="formatter")
def fixture_formatter() -> UniversalPlaintextFormatter:
    """Create a basic formatter instance for testing."""
    return UniversalPlaintextFormatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


@pytest.fixture(name="log_record")
def fixture_log_record() -> logging.LogRecord:
    """Create a basic log record for testing."""
    return logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="/path/to/file.py",
        lineno=42,
        msg="Test message",
        args=(),
        exc_info=None,
    )


def test_basic_formatting_without_extras(
    formatter: UniversalPlaintextFormatter, log_record: logging.LogRecord
) -> None:
    """Test that basic formatting works without extra attributes."""
    formatted = formatter.format(log_record)
    assert "Test message" in formatted
    assert "test_logger" in formatted
    assert "INFO" in formatted


def test_standard_attributes_excluded(
    formatter: UniversalPlaintextFormatter, log_record: logging.LogRecord
) -> None:
    """Test that standard LogRecord attributes are excluded from extras."""
    formatted = formatter.format(log_record)
    # Standard attributes should not appear as extras
    assert "pathname:" not in formatted
    assert "lineno:" not in formatted
    assert "levelname:" not in formatted


def test_extra_attributes_included(
    formatter: UniversalPlaintextFormatter, log_record: logging.LogRecord
) -> None:
    """Test that extra attributes are included in the formatted output."""
    log_record.user_id = "12345"
    log_record.request_id = "abc-def-ghi"

    formatted = formatter.format(log_record)

    assert "user_id: 12345" in formatted
    assert "request_id: abc-def-ghi" in formatted


def test_default_exclude_keys(
    formatter: UniversalPlaintextFormatter, log_record: logging.LogRecord
) -> None:
    """Test that DEFAULT_EXCLUDE_KEYS (message, asctime) are excluded."""
    # Add 'message' and 'asctime' as extra attributes (shouldn't appear in extras)
    log_record.message = "This should be excluded"
    log_record.asctime = "2025-01-01 12:00:00"

    formatted = formatter.format(log_record)

    # These should not appear as extras
    lines = formatted.split("\n")
    extra_lines = [
        line for line in lines if line.strip().startswith(("message:", "asctime:"))
    ]
    assert len(extra_lines) == 0


def test_multiple_extras_formatting(
    formatter: UniversalPlaintextFormatter, log_record: logging.LogRecord
) -> None:
    """Test formatting with multiple extra attributes."""
    log_record.user_id = "12345"
    log_record.session_id = "session-xyz"
    log_record.ip_address = "192.168.1.1"

    formatted = formatter.format(log_record)

    assert "user_id: 12345" in formatted
    assert "session_id: session-xyz" in formatted
    assert "ip_address: 192.168.1.1" in formatted

    # Check that extras are indented
    lines = formatted.split("\n")
    extra_lines = [
        line
        for line in lines
        if any(key in line for key in ("user_id:", "session_id:", "ip_address:"))
    ]
    for line in extra_lines:
        assert line.startswith("    ")


def test_config_exclude_keys_from_file(log_record: logging.LogRecord) -> None:
    """Test that exclude_keys from configuration file are properly excluded."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "logging.conf"
        config_content = """[formatter_plaintextFormatter]
exclude_keys = custom_field, another_field
"""
        config_path.write_text(config_content)

        # Change to temp directory to read config
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)
            formatter = UniversalPlaintextFormatter(fmt="%(message)s")

            # Add attributes that should be excluded
            log_record.custom_field = "should be excluded"
            log_record.another_field = "also excluded"
            log_record.included_field = "should be included"

            formatted = formatter.format(log_record)

            assert "custom_field:" not in formatted
            assert "another_field:" not in formatted
            assert "included_field: should be included" in formatted
        finally:
            os.chdir(original_dir)


def test_config_exclude_keys_with_whitespace(
    log_record: logging.LogRecord,
) -> None:
    """Test that whitespace in exclude_keys configuration is handled correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "logging.conf"
        config_content = """[formatter_plaintextFormatter]
exclude_keys = field1 ,  field2  , field3
"""
        config_path.write_text(config_content)

        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)
            formatter = UniversalPlaintextFormatter(fmt="%(message)s")

            log_record.field1 = "excluded"
            log_record.field2 = "excluded"
            log_record.field3 = "excluded"

            formatted = formatter.format(log_record)

            assert "field1:" not in formatted
            assert "field2:" not in formatted
            assert "field3:" not in formatted
        finally:
            os.chdir(original_dir)


def test_no_config_file(
    formatter: UniversalPlaintextFormatter, log_record: logging.LogRecord
) -> None:
    """Test that formatter works correctly when config file doesn't exist."""
    log_record.some_extra = "value"
    formatted = formatter.format(log_record)

    # Should still format correctly
    assert "Test message" in formatted
    assert "some_extra: value" in formatted


def test_empty_extras(
    formatter: UniversalPlaintextFormatter, log_record: logging.LogRecord
) -> None:
    """Test formatting when there are no extra attributes."""
    formatted = formatter.format(log_record)

    # Should only contain the base formatted message without extra newlines
    lines = formatted.split("\n")
    assert len([line for line in lines if line.strip()]) == 1


def test_standard_attrs_caching(
    formatter: UniversalPlaintextFormatter,
) -> None:
    """Test that standard attributes are cached after first call."""
    assert formatter._standard_attrs is None

    # First call should set the cache
    standard_attrs = formatter._get_standard_attrs()
    assert formatter._standard_attrs is not None
    assert formatter._standard_attrs == standard_attrs

    # Second call should return cached value
    standard_attrs_2 = formatter._get_standard_attrs()
    assert standard_attrs_2 is standard_attrs  # Same object


def test_formatter_with_custom_format_string(
    log_record: logging.LogRecord,
) -> None:
    """Test formatter with a custom format string."""
    formatter = UniversalPlaintextFormatter(fmt="[%(levelname)s] %(message)s")
    log_record.extra_data = "test"

    formatted = formatter.format(log_record)

    assert "[INFO] Test message" in formatted
    assert "extra_data: test" in formatted


def test_exclude_keys_combination(log_record: logging.LogRecord) -> None:
    """Test that all exclusion sources are combined correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "logging.conf"
        config_content = """[formatter_plaintextFormatter]
exclude_keys = config_excluded
"""
        config_path.write_text(config_content)

        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)
            formatter = UniversalPlaintextFormatter(fmt="%(message)s")

            # Add various attributes
            log_record.pathname = "standard_attr"  # Standard LogRecord attribute
            log_record.message = "default_excluded"  # DEFAULT_EXCLUDE_KEYS
            log_record.config_excluded = "from_config"  # From config file
            log_record.should_appear = "yes"  # Should appear

            formatted = formatter.format(log_record)

            # Only should_appear should be in extras
            assert "pathname:" not in formatted  # Standard attribute
            assert "message:" not in formatted  # DEFAULT_EXCLUDE_KEYS
            assert "config_excluded:" not in formatted  # Config exclude
            assert "should_appear: yes" in formatted  # Should be included
        finally:
            os.chdir(original_dir)
