# pyspark-data-quality

A lightweight, composable framework for running data quality checks on PySpark DataFrames. It helps you:

- Split data into valid and invalid subsets per rule
- Emit structured, typed metric results for observability
- Configure severity levels and dimensions (e.g., completeness)
- Cache intermediate values across checks

This repository currently includes a completeness rule example and a minimal framework for adding new checks.

## Requirements

- Python 3.10+
- PySpark 3.x
- pydantic, pydantic-settings

Install core dependencies:

```bash
pip install pyspark pydantic pydantic-settings
```

Because this project uses a src layout and does not yet ship packaging metadata, add the `src` directory to your `PYTHONPATH` while developing:

```bash
export PYTHONPATH="$PWD/src:$PYTHONPATH"
```

## Quickstart

```python
from pyspark.sql import SparkSession, functions as F, Column

from pyspark_data_quality.dq_manager import DQManager
from pyspark_data_quality.checks.completeness_checks.completeness_col_ratio_check import (
    CompletenessColRatioRule,
)
from pyspark_data_quality.core._enums import SeverityLevel

# 1) Create Spark session
spark = SparkSession.builder.master("local[*]").appName("dq-quickstart").getOrCreate()

# 2) Build a sample DataFrame
n = 1000
base = spark.range(0, n)
df = base.select(
    F.col("id").cast("int").alias("name_id"),
    F.concat(F.lit("name_"), F.col("id")).alias("name"),
    (F.floor(F.rand(42) * 10)).cast("int").alias("age"),
    F.concat(F.lit("city_"), F.col("id")).alias("city"),
)

# 3) Define and run checks
manager = DQManager(spark=spark)
manager.set_data(df)
manager.add_check(
    CompletenessColRatioRule(
        dataset="demo",
        run_id="run-001",
        severity_level=SeverityLevel.HIGH,
        metric_name="completeness_ratio",
        input_attributes=["name", "age", "city"],
        threshold=0.95,  # require at least 95% non-null per column
    )
)

result = manager.run()

# 4) Consume outputs
valid_df = result.get_valid_df()
invalid_df = result.get_invalid_df()
metrics_df = result.get_metric_results()

valid_df.show(5)
invalid_df.show(5)
metrics_df.show(truncate=False)
```

## Concepts and API

### DQManager

- Orchestrates rule execution and provides a `ResultObj`.
- Injects a shared cache object into checks via `add_check`.
- API:
  - `set_data(df: DataFrame) -> None`
  - `add_check(check: BaseCheck) -> None`
  - `run() -> ResultObj`

### ResultObj

Provides convenient methods to consume results:
- `get_valid_df() -> DataFrame`: applies all checks to produce the valid subset
- `get_invalid_df() -> DataFrame`: applies all checks to produce the invalid subset
- `get_metric_results() -> DataFrame`: returns a DataFrame of typed metric rows with schema based on `MetricResult`

Typical output columns include:
- `dataset`, `run_id`, `run_ts`, `metric_name`, `column`, `dimension`, `severity_level`
- `threshold_result`, `threshold_range`, `threshold`, `value_double`, `value_string`, `ingest_datetime`

### Checks

This repo includes `CompletenessColRatioRule`:
- Computes the ratio of non-null values per column in `input_attributes`
- Compares the ratio to `threshold` and emits a success/failure message
- Attributes: `dataset`, `run_id`, `metric_name`, `severity_level`, `input_attributes`, `threshold`, `dimension`

Constructor signature (selected):
```python
CompletenessColRatioRule(
    dataset: str,
    run_id: str,
    severity_level: SeverityLevel,
    metric_name: str,
    input_attributes: list[str],
    threshold: float,
    run_datetime: str | None = None,
)
```

## Extending: Write your own check

Create a new class that inherits from `BaseCheck` and implement the abstract methods.

```python
from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame

from pyspark_data_quality.checks.base_check import BaseCheck
from pyspark_data_quality.core._enums import Dimension
from pyspark_data_quality.core.models import MetricResult

class MyCustomRule(BaseCheck):
    def __init__(self, dataset: str, run_id: str, metric_name: str):
        self.dataset = dataset
        self.run_id = run_id
        self.metric_name = metric_name
        self.dimension = Dimension.VALIDITY
        self._df: DataFrame | None = None
        self._valid_df: DataFrame | None = None
        self._invalid_df: DataFrame | None = None

    def set_data(self, df: DataFrame) -> DataFrame:
        self._df = df
        self._valid_df = None
        self._invalid_df = None
        return df

    def get_valid_df(self, df: DataFrame) -> DataFrame:
        self._df = df
        # TODO: return filtered DataFrame containing valid rows
        return df

    def get_invalid_df(self, df: DataFrame) -> DataFrame:
        self._df = df
        # TODO: return filtered DataFrame containing invalid rows
        return df.limit(0)

    def get_metric_results(self) -> list[MetricResult]:
        # TODO: compute and return MetricResult objects
        return []
```

Notes:
- `DQManager.add_check` will set `check.cache_obj` for you; use it to cache expensive results across methods.
- Prefer vectorized Spark functions (`pyspark.sql.functions`) over Python UDFs.

## Configuration

`DQManagerConfig` is powered by `pydantic-settings` and reads `.env` by default. Add project-level config to `pyspark_data_quality/core/config.py` and set values using environment variables or a `.env` file at the repo root.

Example `.env`:
```env
# Example placeholders — define your own keys as you add config
DQ_DEFAULT_SEVERITY=high
```

## Development

- Add `src` to `PYTHONPATH` while iterating:
  ```bash
  export PYTHONPATH="$PWD/src:$PYTHONPATH"
  ```
- Run tests (if you add pytest configuration):
  ```bash
  pytest -q
  ```
- Formatting/linting: this project leverages type hints and Pydantic models; feel free to add `ruff`/`mypy` for stricter checks.

## Roadmap

- Additional built-in checks (validity, uniqueness, freshness, accuracy, consistency)
- Packaging for `pip install`
- Example notebooks and docs site

## License

TBD
