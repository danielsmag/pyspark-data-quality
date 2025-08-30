from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from dq_platform.core.config import DQManagerConfig
from dq_platform.core.cache_obj import CacheObject
from dq_platform.result_obj import ResultObj

if TYPE_CHECKING:
    from dq_platform.checks.base_check import BaseCheck

class DQManager:
    
    config: DQManagerConfig
    cache: CacheObject
    original_df: DataFrame
    transformed_df: DataFrame
    list_of_checks: list[BaseCheck]
 
    def __init__(self) -> None:
        self.config = DQManagerConfig()
        self.cache = CacheObject()
        self.list_of_checks = []

    def add_check(self, check: BaseCheck) -> None:
        check.cache_obj = self.cache
        self.list_of_checks.append(check)
    
    def run(self) -> ResultObj:
        self.transformed_df = self.original_df
        for check in self.list_of_checks:
            self.transformed_df = check.set_data(df=self.transformed_df)
        
        self.cache.set(
            key="dq_manager_transformed_df",
            value=self.transformed_df
        )
        self.cache.set(
            key="dq_manager_original_df",
            value=self.original_df
        )
        
        return ResultObj()
        
    def set_data(self, df: DataFrame) -> None:
        self.original_df = df
       