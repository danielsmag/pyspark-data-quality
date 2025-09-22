from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pyspark_data_quality.utils.utils import singleton

class CacheObjectAbstract(ABC):

    @abstractmethod
    def get(self, key: str, type_check: Any = None, default: Any = None) -> Any | None:
        ...
    
    @abstractmethod
    def set(self, key: str, value: Any, type_check: Any = None) -> None:
        ...

@singleton
class CacheObject(CacheObjectAbstract):

    def __init__(self, **kwargs):
        self.cache: dict[str, Any] = {}

    def get(self, key: str, type_check: Any = None, default: Any = None) -> Any | None:
        value = self.cache.get(key)
        if type_check is not None:
            if not isinstance(value, type_check):
                raise ValueError(f"Value for key {key} is not of type {type_check}")
        return value if value is not None else default

    def load(self, path: str) -> None:
        pass
    def save(self, path: str) -> None:
        pass
    
    def set(self, key: str, value: Any, type_check: Any = None) -> None:
        if type_check is not None:
            if not isinstance(value, type_check):
                raise ValueError(f"Value for key {key} is not of type {type_check}")
        self.cache[key] = value