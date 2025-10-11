from __future__ import annotations
from pyspark.rdd import RDD

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, MapType

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark_data_quality.core.models import MetricResult
    from pyspark_data_quality.checks.base_check import BaseCheck

__all__: list[str] = ["ResultObj"]

schema: StructType = StructType([
            StructField("dataset", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("run_ts", TimestampType(), False),
            StructField("metric_name", StringType(), False),
            StructField("column", StringType(), False),
            StructField("dimension", StringType(), False),
            StructField("severity_level", StringType(), False),
            StructField("threshold_result", DoubleType(), True),
            StructField("threshold_range", DoubleType(), True),
            StructField("threshold", DoubleType(), True),
            StructField("value_double", DoubleType(), True),
            StructField("value_string", StringType(), True),
            StructField("ingest_datetime", TimestampType(), False),
            StructField("extra_info", MapType(StringType(), StringType()), True),
        ])
class ResultObj:
    def __init__(
        self,
        list_of_checks: list[BaseCheck],
        df: DataFrame,
        spark: SparkSession
    ) -> None:
        self.list_of_checks: list[BaseCheck] = list_of_checks
        self.df: DataFrame = df
        self._valid_df: DataFrame | None = None
        self._invalid_df: DataFrame | None = None
        self.spark: SparkSession = spark
        
    def get_valid_df(self) -> DataFrame:
        valid_df: DataFrame = self.df
        for check in self.list_of_checks:
            valid_df = check.get_valid_df(df=valid_df)
            assert isinstance(valid_df, DataFrame)
        self._valid_df = valid_df
        return self._valid_df
    
    def get_invalid_df(self) -> DataFrame:
        invalid_df: DataFrame = self.df
        for check in self.list_of_checks:
            invalid_df = check.get_invalid_df(df=invalid_df)
            assert isinstance(invalid_df, DataFrame)
        self._invalid_df = invalid_df
        return self._invalid_df
    
    def get_metric_results(self) -> DataFrame:
        metric_results: list[MetricResult] = []
        for check in self.list_of_checks:
            metric_results.extend(check.get_metric_results())
            
        rows: list[dict] = [result.model_dump() for result in metric_results]
        if rows:
            return self.spark.createDataFrame(rows, schema) # type: ignore
        
        empty_rdd: RDD[Any] = self.spark.sparkContext.emptyRDD()
        return self.spark.createDataFrame(empty_rdd, schema)