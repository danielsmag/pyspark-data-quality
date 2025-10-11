import pytest
from pyspark.sql import Row

from pyspark_data_quality.checks.uniqueness.unique_chek import UniqueCheck
from pyspark_data_quality.core._enums import SeverityLevel
from pyspark_data_quality.core.exceptation import ColumnNotFoundError


def test_unique_check_valid_and_invalid(spark_session) -> None:
    dataframe = spark_session.createDataFrame(
        [
            Row(id=1, name="a"),
            Row(id=1, name="a"),  # duplicate on id
            Row(id=2, name="b"),
            Row(id=2, name="c"),  # duplicate on id with different name
        ]
    )

    check = UniqueCheck(
        dataset="test_ds",
        run_id="run_1",
        severity_level=SeverityLevel.LOW,
        metric_name="uniq_test",
        input_attributes=["id"],
        threshold=0.0,
    )

    deduplicated_df = check.valid(df=dataframe, cols=["id"])
    assert {row["id"] for row in deduplicated_df.collect()} == {1, 2}

    duplicate_groups_df = check.invalid(df=dataframe, cols=["id"])
    duplicates = {(row["id"], row["count"]) for row in duplicate_groups_df.collect()}
    assert duplicates == {(1, 2), (2, 2)}


def test_unique_check_missing_column_raises(spark_session) -> None:
    dataframe = spark_session.createDataFrame([Row(id=1)])

    check = UniqueCheck(
        dataset="test_ds",
        run_id="run_2",
        severity_level=SeverityLevel.LOW,
        metric_name="uniq_test",
        input_attributes=["id", "missing_col"],
        threshold=0.0,
    )

    with pytest.raises(ColumnNotFoundError):
        _ = check.valid(df=dataframe, cols=["id"])  # triggers _pre_check


