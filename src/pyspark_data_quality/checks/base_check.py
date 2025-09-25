from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from pyspark.sql import DataFrame
from pyspark_data_quality.core.models import MetricResult
from pyspark_data_quality.core.cache_obj import CacheObject

class AbstractCheck(ABC):
    
    @abstractmethod
    def get_metric_results(self) -> list[MetricResult]:
        ...

    @property
    def cache_obj(self) -> CacheObject:
        ...
    
    @cache_obj.setter
    def cache_obj(self, cache_obj: CacheObject) -> None:
        ...
        
    @abstractmethod
    def get_valid_df(self,df: DataFrame) -> DataFrame:
        ...
    
    @abstractmethod
    def get_invalid_df(self,df: DataFrame) -> DataFrame:
        ...
        
    @abstractmethod
    def valid(self,*, df: DataFrame, cols: list[str] = [],col: str = "") -> DataFrame:
        ...
    
    @abstractmethod
    def invalid(self,*, df: DataFrame, cols: list[str] = [],col: str = "") -> DataFrame:
        ...
    
class BaseCheck(AbstractCheck):
    pass