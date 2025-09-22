from __future__ import annotations

from pyspark.sql import functions as F, DataFrame, Row
from typing import List, Any, Literal

from datetime import datetime

from pyspark_data_quality.checks.base_check import BaseCheck
from pyspark_data_quality.core.cache_obj import CacheObject
from pyspark_data_quality.core._enums import SeverityLevel, Dimension, CheckStatus
from pyspark_data_quality.core.exceptation import ColumnNotFoundError
from pyspark_data_quality.core.models import MetricResult


__all__: list[str] = ["CompletenessColRatioRule"]


class CompletenessColRatioRule(BaseCheck):
    dataset: str
    run_id: str
    _cache_obj: CacheObject | None
    severity_level: SeverityLevel
    metric_name: str
    input_attributes: List[str]
    threshold: float
    check_type: str
    dimension: Dimension
    run_datetime: str
    _df_count: int | None
    _df: DataFrame | None
    _valid_df: DataFrame | None
    _invalid_df: DataFrame | None
    _metric_result: list[MetricResult]

    def __init__(
        self,
        dataset: str,
        run_id: str,
        severity_level: SeverityLevel,
        metric_name: str,
        input_attributes: List[str],
        threshold: float,
        run_datetime: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ) -> None:
        self.dataset = dataset
        self.run_id = run_id
        self.check_type = "completeness"
        self.dimension = Dimension.COMPLETENESS
        self.severity_level = severity_level
        self.metric_name = metric_name
        self.input_attributes = input_attributes
        self.threshold = threshold 
        self.run_datetime = run_datetime
        self._cache_obj = None
        self._df_count = None
        self._df = None
        self._valid_df = None
        self._invalid_df = None
        self._metric_result = []

    def _pre_check(self, df: DataFrame) -> None:
        missing: List[str] = [c for c in self.input_attributes if c not in df.columns]
        if missing:
            raise ColumnNotFoundError(f"Columns not found: {missing}")

    def set_data(self, df: DataFrame) -> DataFrame:
        self._pre_check(df)
        self.df = df
        self._prepare_df()
        self._valid_df = None
        self._invalid_df = None
        self._metric_result = []
        self._df_count = None
        return self.df
    
    def _prepare_df(self) -> None:
        df: DataFrame = self.df
     
        for c in self.input_attributes:
            if c not in df.columns:
                raise ColumnNotFoundError(f"Columns not found: {c}")
            
    def valid(self, df: DataFrame, col: str) -> DataFrame:
        self._pre_check(df)
        return df.filter(F.col(col).isNotNull())

    def invalid(self, df: DataFrame, col: str) -> DataFrame:
        return df.filter(F.col(col).isNull())

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
            for col in self.input_attributes:
                self._valid_df = self.valid(self.df, col)
        assert isinstance(self._valid_df, DataFrame)
        return self._valid_df

    def get_invalid_df(self,df: DataFrame) -> DataFrame:
        self.df = df
        if self._invalid_df is None:
            for col in self.input_attributes:
                self._invalid_df = self.invalid(self.df, col)
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

    def metric_results(self) -> list[MetricResult]:
        self._pre_check(self.df)
          
        ratios: Row | None = self.df.select([
        F.avg(F.col(c).isNotNull().cast("double")).alias(c)
        for c in self.input_attributes
        ]).first()
        
        if ratios is None:
            raise ValueError("No count results found")
        
        ratios_dict: dict = ratios.asDict()
        
        out: list[MetricResult] = []
        now: datetime = datetime.now()
        for col in self.input_attributes:
            pct: float = float(ratios_dict.get(col, 0.0))  
            status: Literal[CheckStatus.SUCCESS, CheckStatus.FAILURE] = CheckStatus.SUCCESS if pct >= self.threshold else CheckStatus.FAILURE
            msg: str = (f"{col} completeness {pct:.2%} >= {self.threshold:.2%}"
                if status == CheckStatus.SUCCESS
                else f"{col} completeness {pct:.2%} < {self.threshold:.2%}")

            out.append(MetricResult(
                dataset=self.dataset,
                run_id=self.run_id,
                run_ts=now,
                metric_name=self.metric_name,
                column=col,
                dimension=self.dimension.value,
                severity_level=self.severity_level.value,
                threshold_result=pct,
                threshold=self.threshold,
                value_double=pct,
                value_string=msg,
                ingest_datetime=now,
            ))
        self._metric_result = out
        return out


