from __future__ import annotations

from pyspark.sql.column import Column
from pyspark.sql import functions as F, DataFrame, Row
from typing import Callable, Literal
from typing_extensions import override
from datetime import datetime

from pyspark_data_quality.checks.base_check import BaseCheck
from pyspark_data_quality.core._enums import SeverityLevel, Dimension, CheckStatus
from pyspark_data_quality.core.exceptation import ColumnNotFoundError
from pyspark_data_quality.core.models import MetricResult


__all__: list[str] = ["CompletenessColRatioRule"]


class CompletenessColRatioRule(BaseCheck):
    """
    Completeness Column Ratio Check
    Computes the ratio of non-null values per column in `input_attributes`
    Compares the ratio to `threshold` and emits a success/failure message
    Attributes: `dataset`, `run_id`, `metric_name`, `severity_level`, `input_attributes`, `threshold`, `dimension`
    """
    
    dataset: str
    run_id: str
    severity_level: SeverityLevel
    metric_name: str
    input_attributes: list[str]
    threshold: float
    check_type: str
    dimension: Dimension
    run_datetime: str
    condition: Callable[..., Column] | Column | None = None
  

    def __init__(
        self,
        dataset: str,
        run_id: str,
        severity_level: SeverityLevel,
        metric_name: str,
        input_attributes: list[str],
        threshold: float,
        run_datetime: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        condition: Callable[..., Column] | Column | None = None,
    ) -> None:
        super().__init__()
        self.dataset = dataset
        self.run_id = run_id
        self.check_type = "completeness"
        self.dimension = Dimension.COMPLETENESS
        self.severity_level = severity_level
        self.metric_name = metric_name
        self.input_attributes = input_attributes
        self.threshold = threshold 
        self.run_datetime = run_datetime
        self.condition = condition
        
    def _pre_check(self, df: DataFrame) -> None:
        missing: list[str] = [c for c in self.input_attributes if c not in df.columns]
        if missing:
            raise ColumnNotFoundError(f"Columns not found: {missing}")
    
    def _condition_to_col(self, df: DataFrame) -> Column:
        if self.condition is None:
            return F.lit(True)  
        if isinstance(self.condition, Column):
            return self.condition
        if callable(self.condition):
            out: Column = self.condition(df)
            if not isinstance(out, Column):
                raise TypeError("Callable condition must return a Column.")
            return out
        raise TypeError("condition must be Column | dict | Callable[[DataFrame], Column] | None")
    
    @override
    def valid(self,*, df: DataFrame, cols: list[str] = [],col: str = "") -> DataFrame:
        self._pre_check(df)
        scope: Column = self._condition_to_col(df)
        if col:
            return df.filter(F.col(col).isNotNull() & scope)
        if cols:
            condition: Column = F.lit(True)
            for c in cols:
                condition = condition & F.col(c).isNotNull()
            return df.filter(condition & scope)
        return df.filter(scope)

    @override
    def invalid(self,*, df: DataFrame, cols: list[str] = [],col: str = "") -> DataFrame:
        scope: Column = self._condition_to_col(df)
        if col:
            return df.filter(F.col(col).isNull() & scope)
        if cols:
            condition: Column = F.lit(False)
            for c in cols:
                condition = condition | F.col(c).isNull()
            return df.filter(condition & scope)
        return df.filter(scope & F.lit(False))

    def metric_results(self) -> list[MetricResult]:
        self._pre_check(self.df)
        scope: Column = self._condition_to_col(self.df)
        filtered_df: DataFrame = self.df.filter(scope)
        ratios_row: Row | None = filtered_df.select([
            F.avg(F.col(c).isNotNull().cast("double")).alias(c)
            for c in self.input_attributes
        ]).first()
        if ratios_row is None:
            raise ValueError("No count results found")
        
        ratios_dict: dict = ratios_row.asDict()
        
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
                extra_info={"condition": str(self.condition) if self.condition is not None else None},
            ))
        self._metric_result = out
        return out


