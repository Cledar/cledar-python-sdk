"""
Pytest configuration and fixtures to ensure proper teardown of Kafka clients and
their background threads during tests.
"""

import threading
import time
import weakref
from collections.abc import Callable, Generator
from typing import Any, cast

import pytest

from cledar.kafka.clients.base import BaseKafkaClient
from cledar.kafka.logger import logger

# Weak registry of all created BaseKafkaClient instances (does not keep them alive)
_active_clients: "weakref.WeakSet[BaseKafkaClient]" = weakref.WeakSet()


def _wrap_post_init() -> tuple[
    Callable[[BaseKafkaClient], None], Callable[[BaseKafkaClient], None]
]:
    """Monkeypatch BaseKafkaClient.__post_init__ to register instances."""
    original = BaseKafkaClient.__post_init__

    def wrapped(self: BaseKafkaClient) -> None:
        original(self)
        try:
            _active_clients.add(self)
        except Exception:
            # Best-effort registration only for tests
            pass

    return original, wrapped


def _wrap_start_connection_check_thread() -> tuple[
    Callable[[BaseKafkaClient], None], Callable[[BaseKafkaClient], None]
]:
    """Monkeypatch start_connection_check_thread to use daemon threads in tests.

    This prevents non-daemon threads from blocking interpreter shutdown when tests
    forget to call shutdown() explicitly.
    """
    original = BaseKafkaClient.start_connection_check_thread

    def wrapped(self: BaseKafkaClient) -> None:
        if self.connection_check_thread is None:
            self.connection_check_thread = threading.Thread(
                target=self._monitor_connection
            )
            # Ensure test background threads never block process exit
            self.connection_check_thread.daemon = True
            self.connection_check_thread.start()
            logger.info(
                f"Started {self.__class__.__name__} connection check thread.",
                extra={"interval": self.config.kafka_connection_check_interval_sec},
            )

    return original, wrapped


def _cleanup_all_clients() -> None:
    """Shutdown all known clients; ignore errors during cleanup."""
    for client in list(_active_clients):
        try:
            client.shutdown()
        except Exception:
            pass


@pytest.fixture(scope="session", autouse=True)
def _session_monkeypatch() -> Generator[None, None, None]:
    """Apply monkeypatches for the entire test session and ensure final cleanup."""
    # Monkeypatch __post_init__ to register instances
    orig_post_init, wrapped_post_init = _wrap_post_init()
    cast(Any, BaseKafkaClient).__post_init__ = wrapped_post_init

    # Monkeypatch start_connection_check_thread to create daemon threads
    orig_start, wrapped_start = _wrap_start_connection_check_thread()
    cast(Any, BaseKafkaClient).start_connection_check_thread = wrapped_start

    try:
        yield
    finally:
        # Restore originals
        cast(Any, BaseKafkaClient).__post_init__ = orig_post_init
        cast(Any, BaseKafkaClient).start_connection_check_thread = orig_start

        # Final cleanup at session end
        _cleanup_all_clients()

        # Give threads a small grace period to finish
        time.sleep(0.1)


@pytest.fixture(autouse=True)
def _per_test_cleanup() -> Generator[None, None, None]:
    """Ensure all clients are shut down after each individual test."""
    yield
    _cleanup_all_clients()
    # Small grace to allow quick thread exit
    time.sleep(0.05)
