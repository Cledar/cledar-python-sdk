from typing import TypeVar

from dataclasses import dataclass


@dataclass
class BaseConfigClass:
    pass


ConfigAbstract = TypeVar("ConfigAbstract", bound=BaseConfigClass)
