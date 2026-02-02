"""Base models for Redis-based configuration."""

from dataclasses import dataclass
from typing import TypeVar


@dataclass
class BaseConfigClass:
    """Base class for configuration models stored in Redis."""

    pass


ConfigAbstract = TypeVar("ConfigAbstract", bound=BaseConfigClass)
