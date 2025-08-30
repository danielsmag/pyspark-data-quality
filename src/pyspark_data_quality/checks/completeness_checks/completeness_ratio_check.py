from __future__ import annotations

from pyspark.sql import functions as F, DataFrame, Column
from typing import List
from functools import reduce
from operator import or_
from datetime import datetime

from dq_platform.checks.base_check import BaseCheck
from dq_platform.core.cache_obj import CacheObject
from dq_platform.core._enums import SeverityLevel, Dimension, CheckStatus
from dq_platform.core.exceptation import ColumnNotFoundError
from dq_platform.result_obj import MetricResult

# ... your imports ...

__all__: list[str] = ["CompletenessRawRatioRule"]


class CompletenessRawRatioRule(BaseCheck):
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
    _columns_added: List[str]
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
        self.threshold = threshold  # expected 0.0–1.0
        self.run_datetime = run_datetime

        self._columns_added = []
        self._col_suffix = "__pct_non_null"  # actually 0/1 flags; keep name for compatibility
        self._row_ratio_col = "__row_completeness_ratio"
        self._row_valid_col = "__row_is_valid"

        self._cache_obj = None
        self._df_count = None
        self._df = None
        self._valid_df = None
        self._invalid_df = None
        self._metric_result = []

    # ---------- Setup ----------

    def _pre_check(self, df: DataFrame) -> None:
        missing: List[str] = [c for c in self.input_attributes if c not in df.columns]
        if missing:
            raise ColumnNotFoundError(f"Columns not found: {missing}")

    def set_data(self, df: DataFrame) -> DataFrame:
        self._pre_check(df)
        self.df = df
        # add indicator cols + row ratio + row validity
        self._prepare_df()
        # reset cached splits/metrics
        self._valid_df = None
        self._invalid_df = None
        self._metric_result = []
        self._df_count = None
        return self.df

    # one 0/1 indicator per input column
    def _indicator_name(self, col: str) -> str:
        return f"{col}{self._col_suffix}"

    def expression(self, col: str, alias: str) -> Column:
        # keep your interface; returns 0/1 indicator
        return F.when(F.col(col).isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0)).alias(alias)

    def _prepare_df(self) -> None:
        df: DataFrame = self.df

        # add per-column 0/1 indicators
        added_cols = []
        for c in self.input_attributes:
            name = self._indicator_name(c)
            df = df.withColumn(name, self.expression(c, name))
            added_cols.append(name)
        self.columns_added = added_cols
        print(added_cols)
        df.show(10)
        # per-row completeness ratio = sum(indicators) / number_of_attributes
        if not added_cols:
            raise ValueError("No input_attributes provided for completeness calculation.")

        summed: Column = F.aggregate(
            F.array(*[F.col(c) for c in added_cols]),
            F.lit(0.0),
            lambda acc, x: acc + x,
        )
        print(summed)
        
        df = df.withColumn(self._row_ratio_col, (summed / F.lit(len(added_cols))))

        # row validity by threshold
        df = df.withColumn(self._row_valid_col, F.col(self._row_ratio_col) >= F.lit(self.threshold))

        self._df = df

    # ---------- Conditions / Splits ----------

    def _invalid_cond(self) -> Column:
        # kept for compatibility, but you now classify via row ratio, not "any null"
        return ~F.col(self._row_valid_col)

    def valid(self, df: DataFrame) -> DataFrame:
        self._pre_check(df)
        # use row-level validity (>= threshold), not "drop any null"
        return df.filter(F.col(self._row_valid_col))

    def invalid(self, df: DataFrame) -> DataFrame:
        # complement of valid
        return df.filter(~F.col(self._row_valid_col))

    # ---------- Public accessors ----------

    @property
    def df_count(self) -> int:
        c: int | None = self.cache_obj.get("df_count", default=None)
        if c is None:
            c = self.df.count()
            self.cache_obj.set("df_count", c)
        return c

    @property
    def df(self) -> DataFrame:
        if self._df is None:
            raise ValueError("DataFrame is not set")
        return self._df

    @df.setter
    def df(self, df: DataFrame) -> None:
        self._df = df

    @property
    def valid_df(self) -> DataFrame:
        if self._valid_df is None:
            self._valid_df = self.valid(self.df)
        return self._valid_df

    @property
    def invalid_df(self) -> DataFrame:
        if self._invalid_df is None:
            self._invalid_df = self.invalid(self.df)
        return self._invalid_df

    def get_metric_results(self) -> list[MetricResult]:
        return self._metric_result

    @property
    def columns_added(self) -> List[str]:
        return self._columns_added

    @columns_added.setter
    def columns_added(self, columns: List[str]) -> None:
        self._columns_added = columns

    @property
    def cache_obj(self) -> CacheObject:
        if not self._cache_obj:
            raise ValueError("Cache object is not set")
        return self._cache_obj

    @cache_obj.setter
    def cache_obj(self, cache_obj: CacheObject) -> None:
        self._cache_obj = cache_obj

    # ---------- Metrics ----------

    def metric_results(self) -> list[MetricResult]:
        """
        Per-column completeness = avg(0/1 indicator).
        Single aggregation job for all columns.
        """
        if not self.columns_added:
            return []

        # build one agg pass
        agg_exprs = [F.avg(F.col(col)).alias(col) for col in self.columns_added]
        avg_row = self.df.agg(*agg_exprs).collect()[0].asDict()

        for ind_col in self.columns_added:
            pct = float(avg_row[ind_col])  # 0.0–1.0
            status: CheckStatus = (
                CheckStatus.SUCCESS if pct >= self.threshold else CheckStatus.FAILURE
            )
            msg: str = (
                f"{ind_col} completeness {pct:.2%} >= {self.threshold:.2%}"
                if status == CheckStatus.SUCCESS
                else f"{ind_col} completeness {pct:.2%} < {self.threshold:.2%}"
            )
            self._metric_result.append(
                MetricResult(
                    dataset=self.dataset,
                    run_id=self.run_id,
                    run_ts=datetime.now(),
                    metric_name=self.metric_name,
                    column=ind_col,
                    dimension=self.dimension.value,
                    severity_level=self.severity_level.value,
                    threshold_result=pct,
                    threshold=self.threshold,
                    value_double=pct,
                    value_string=msg,
                    ingest_datetime=datetime.now(),
                )
            )
        return self._metric_result






# from __future__ import annotations

# from pyspark.sql import functions as F, DataFrame, Column
# from typing import List

# from dq_platform.core.cache_obj import CacheObject
# from dq_platform.core._enums import SeverityLevel, Dimension, CheckStatus
# from dq_platform.core.exceptation import ColumnNotFoundError
# from datetime import datetime
# from dq_platform.checks.base_check import BaseCheck
# from dq_platform.core.models import MetricResult
# from functools import reduce
# from operator import and_, or_

# __all__: list[str] = ["CompletenessRatioRule"]


# class CompletenessRatioRule(BaseCheck):
#     dataset: str
#     run_id: str
#     _cache_obj: CacheObject | None
#     severity_level: SeverityLevel
#     metric_name: str    
#     input_attributes: List[str]
#     threshold: float
#     check_type: str
#     dimension: Dimension
#     run_datetime: str
#     _columns_added: List[str]
#     _df_count: int | None
#     _df: DataFrame | None
#     _valid_df: DataFrame | None    
#     _invalid_df: DataFrame | None
#     _metric_result: list[MetricResult]

#     def __init__(
#         self,
#         dataset: str,
#         run_id: str,        
#         severity_level: SeverityLevel,
#         metric_name: str,
#         input_attributes: List[str],
#         threshold: float,
#         run_datetime: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#     ) -> None:
#         self.dataset: str = dataset
#         self.run_id: str = run_id
#         self.check_type: str = "completeness"
#         self.dimension = Dimension.COMPLETENESS
#         self.severity_level = severity_level
#         self.metric_name = metric_name
#         self.input_attributes = input_attributes
#         self.threshold = threshold
#         self.run_datetime = run_datetime
#         self._columns_added = []
#         self._col_suffix = "__pct_non_null"
#         self._cache_obj = None
#         self._df_count = None
#         self._df = None
#         self._valid_df = None
#         self._invalid_df = None
#         self._metric_result = []
        
#     def _pre_check(self, df: DataFrame) -> None:
#         missing: List[str] = [c for c in (self.input_attributes) if c not in df.columns]
#         if missing:
#             raise ColumnNotFoundError(f"Columns not found: {missing}")
    
#     def set_data(self, df: DataFrame) -> DataFrame:
#         self._pre_check(df)
#         self.df = df
#         return self.df
    
#     def expression(self, col: str, alias: str) -> Column:
#         expr: Column = (
#             F.when(F.col(col).isNotNull(), F.lit(1))
#                 .otherwise(F.lit(0))
#                 .alias(alias)
#         )
#         return expr

#     def _invalid_cond(self) -> Column:
#         return reduce(or_, (F.col(c).isNull() for c in self.input_attributes), F.lit(False))

    
#     def valid(self, df: DataFrame) -> DataFrame:
#         self._pre_check(df)
#         return df.na.drop(subset=self.input_attributes)
    
#     def invalid(self, df: DataFrame) -> DataFrame:
#         return df.na.drop(subset=self.input_attributes)
    
#     def metric_results(self) -> list[MetricResult]:
#         for col in self.columns_added:
#             pct: float = self.valid_df.filter(F.col(col) == 1).count() / self.df_count
#             status: CheckStatus = (
#                 CheckStatus.SUCCESS if pct >= self.threshold else CheckStatus.FAILURE
#             )
#             msg: str = (
#                 f"{col} completeness {pct:.2%} >= {self.threshold:.2%}"
#                 if status == CheckStatus.SUCCESS
#                 else f"{col} completeness {pct:.2%} < {self.threshold:.2%}"
#             )
        
#             self._metric_result.append(
#                 MetricResult(
#                     dataset=self.dataset,
#                     run_id=self.run_id,
#                     run_ts=datetime.now(),
#                     metric_name=self.metric_name,
#                     column=col,
#                     dimension=self.dimension.value,
#                     severity_level=self.severity_level.value,
#                     threshold_result=pct,
#                     threshold=self.threshold,
#                     value_double=pct,
#                     value_string=msg,
#                     ingest_datetime=datetime.now()
#                 ))
#         return self._metric_result
    
#     @property
#     def df_count(self) -> int:
#         c: int | None = self.cache_obj.get("df_count",default=None)
#         if c is None:
#             c = self.df.count()
#             self.cache_obj.set("df_count", c)
#         return c
    
#     @property
#     def df(self) -> DataFrame:
#         if not self._df:
#             raise ValueError("DataFrame is not set")
#         return self._df
    
#     @df.setter
#     def df(self, df: DataFrame) -> None:
#         self._df = df
    
#     @property
#     def valid_df(self) -> DataFrame:
#         if not self._valid_df:
#             self._valid_df = self.valid(df=self.df)
#         return self._valid_df
    
#     @property
#     def invalid_df(self) -> DataFrame:
#         if not self._invalid_df:
#             self._invalid_df = self.invalid(df=self.df)
#         return self._invalid_df
    
#     def get_metric_results(self) -> list[MetricResult]:
#         return self._metric_result
    
#     @property
#     def columns_added(self) -> List[str]:
#         return self._columns_added
    
#     @columns_added.setter
#     def columns_added(self, columns: List[str]) -> None:
#         self._columns_added = columns
    
#     @property
#     def cache_obj(self) -> CacheObject:
#         if not self._cache_obj:
#             raise ValueError("Cache object is not set")
#         return self._cache_obj
    
#     @cache_obj.setter
#     def cache_obj(self, cache_obj: CacheObject) -> None:
#         self._cache_obj = cache_obj