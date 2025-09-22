from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession

from pyspark_data_quality.core.config import DQManagerConfig
from pyspark_data_quality.core.cache_obj import CacheObject
from pyspark_data_quality.result_obj import ResultObj

if TYPE_CHECKING:
    from pyspark_data_quality.checks.base_check import BaseCheck

class DQManager:
    
    config: DQManagerConfig
    cache: CacheObject
    original_df: DataFrame
    transformed_df: DataFrame
    list_of_checks: list[BaseCheck]
 
    def __init__(self,spark: SparkSession) -> None:
        self.config = DQManagerConfig()
        self.cache = CacheObject()
        self.list_of_checks = []
        self.spark = spark

    def add_check(self, check: BaseCheck) -> None:
        check.cache_obj = self.cache
        self.list_of_checks.append(check)
    
    def run(self) -> ResultObj:
        self.transformed_df = self.original_df
        self.cache.set(
            key="dq_manager_transformed_df",
            value=self.transformed_df
        )
        self.cache.set(
            key="dq_manager_original_df",
            value=self.original_df
        )
        
        return ResultObj(
            list_of_checks=self.list_of_checks,
            df=self.transformed_df,
            spark=self.spark
        )
        
    def set_data(self, df: DataFrame) -> None:
        self.original_df = df
       
   