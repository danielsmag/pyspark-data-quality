from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from pyspark.sql import DataFrame
from pyspark_data_quality.core.models import MetricResult
from pyspark_data_quality.core.cache_obj import CacheObject
from typing import Any
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
        
    @abstractmethod
    def metric_results(self) -> list[MetricResult]:
        ...
    
class BaseCheck(AbstractCheck):
    _df_count: int | None
    _df: DataFrame | None
    _valid_df: DataFrame | None
    _invalid_df: DataFrame | None
    _metric_result: list[MetricResult]
    _cache_obj: CacheObject | None
    threshold: float
    input_attributes: list[str]
    
    def __init__(self) -> None:
        self._cache_obj = None
        self._df_count = None
        self._df = None
        self._valid_df = None
        self._invalid_df = None
        self._metric_result = [] 
        self.input_attributes = []
        
    @property
    def df_count(self) -> int:
        if self.cache_obj.get("df_count") is None:
            self.cache_obj.set("df_count", self.df.count())
        c: Any = self.cache_obj.get("df_count",type_check=int)
        assert isinstance(c, int)
        return c
    
    @property
    def df(self) -> DataFrame:
        if self._df is None:
            raise ValueError("DataFrame is not set")
        return self._df
    
    @df.setter
    def df(self, df: DataFrame) -> None:
        self._df = df

    def get_valid_df(self,df: DataFrame) -> DataFrame:
        self.df = df
        if self._valid_df is None:
            self._valid_df = self.valid(df=self.df, cols=self.input_attributes)
        assert isinstance(self._valid_df, DataFrame)
        return self._valid_df
    
    def get_invalid_df(self,df: DataFrame) -> DataFrame:
        self.df = df
        if self._invalid_df is None:
            self._invalid_df = self.invalid(df=self.df, cols=self.input_attributes)
        assert isinstance(self._invalid_df, DataFrame)
        return self._invalid_df

    def get_metric_results(self) -> list[MetricResult]:
        self._metric_result = self.metric_results()
        return self._metric_result
    
    @property
    def cache_obj(self) -> CacheObject:
        if not self._cache_obj:
            raise ValueError("Cache object is not set")
        return self._cache_obj

    @cache_obj.setter
    def cache_obj(self, cache_obj: CacheObject) -> None:
        self._cache_obj = cache_obj

    @property
    def valid_df(self) -> DataFrame:
        if self._valid_df is None:
            self._valid_df = self.valid(df=self.df, cols=self.input_attributes)
        assert isinstance(self._valid_df, DataFrame)
        return self._valid_df
    
    @property
    def invalid_df(self) -> DataFrame:
        if self._invalid_df is None:
            self._invalid_df = self.invalid(df=self.df, cols=self.input_attributes)
        assert isinstance(self._invalid_df, DataFrame)
        return self._invalid_df