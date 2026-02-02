"""Example usage of RedisConfigStore."""

from dataclasses import dataclass
from typing import Any

from .model import BaseConfigClass
from .redis_config_store import RedisConfigStore


@dataclass
class ExampleConfig(BaseConfigClass):
    """Example configuration model."""

    name: str
    index: int
    data: dict[str, Any]


DEFAULT_CONFIG = ExampleConfig(name="name", index=0, data={})
CONFIG_KEY = "example_config"


class ConfigProvider:
    """Example provider for configuration using RedisConfigStore."""

    def __init__(self, redis_config_store: RedisConfigStore) -> None:
        """Initialize the ConfigProvider.

        Args:
            redis_config_store: The Redis config store instance.
        """
        self.redis_config_store = redis_config_store
        if self.redis_config_store.fetch(ExampleConfig, CONFIG_KEY) is None:
            self.redis_config_store[CONFIG_KEY] = DEFAULT_CONFIG

    def get_example_config(self) -> ExampleConfig:
        """Get the example configuration.

        Returns:
            ExampleConfig: The current configuration.
        """
        return (
            self.redis_config_store.fetch(ExampleConfig, CONFIG_KEY) or DEFAULT_CONFIG
        )

    def get_example_config_version(self) -> int:
        """Get the version of the example configuration.

        Returns:
            int: The configuration version.
        """
        return self.redis_config_store.cached_version(CONFIG_KEY) or -1

    def set_example_config(self, config: ExampleConfig | None) -> None:
        """Set the example configuration.

        Args:
            config: The new configuration to set.
        """
        if config is None:
            return

        self.redis_config_store[CONFIG_KEY] = config
