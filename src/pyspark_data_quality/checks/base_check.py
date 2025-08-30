from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from pyspark.sql import DataFrame
from dq_platform.result_obj import MetricResult
from dq_platform.core.cache_obj import CacheObject

class AbstractCheck(ABC):
    @abstractmethod
    def set_data(self, df: DataFrame) -> DataFrame:
        ...
    
    @abstractmethod
    def get_metric_results(self) -> list[MetricResult]:
        ...

    @property
    def cache_obj(self) -> CacheObject:
        ...
    
    @cache_obj.setter
    def cache_obj(self, cache_obj: CacheObject) -> None:
        ...
    
class BaseCheck(AbstractCheck):
    pass