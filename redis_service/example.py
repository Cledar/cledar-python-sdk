from dataclasses import dataclass
from typing import Any

from .model import BaseConfigClass
from .redis_config_store import RedisConfigStore


@dataclass
class ExampleConfig(BaseConfigClass):
    name: str
    index: int
    data: dict[str, Any]


DEFAULT_CONFIG = ExampleConfig(name="name", index=0, data={})
CONFIG_KEY = "example_config"


class ConfigProvider:
    def __init__(self, redis_config_store: RedisConfigStore) -> None:
        self.redis_config_store = redis_config_store
        if self.redis_config_store.fetch(ExampleConfig, CONFIG_KEY) is None:
            self.redis_config_store[CONFIG_KEY] = DEFAULT_CONFIG

    def get_example_config(self) -> ExampleConfig:
        return (
            self.redis_config_store.fetch(ExampleConfig, CONFIG_KEY) or DEFAULT_CONFIG
        )

    def get_example_config_version(self) -> int:
        return self.redis_config_store.cached_version(CONFIG_KEY) or -1

    def set_example_config(self, config: ExampleConfig | None) -> None:
        if config is None:
            return

        self.redis_config_store[CONFIG_KEY] = config
