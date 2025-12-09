from dataclasses import dataclass
from typing import TypeVar


@dataclass
class BaseConfigClass:
    pass


ConfigAbstract = TypeVar("ConfigAbstract", bound=BaseConfigClass)
