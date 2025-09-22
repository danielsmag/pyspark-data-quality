from pyspark.sql.dataframe import DataFrame


from pyspark_data_quality.dq_manager import DQManager
from pyspark_data_quality.checks.completeness_checks.completeness_col_ratio_check import CompletenessColRatioRule
from pyspark_data_quality.result_obj import ResultObj
from pyspark_data_quality.core._enums import SeverityLevel
from pyspark.sql import SparkSession, Column
from pyspark.sql import functions as F



def create_dataframe_fast(spark: SparkSession, n: int = 900_000, partitions: int | None = None) -> DataFrame:
    # Generate 0..n-1 entirely on the JVM
    df: DataFrame = spark.range(0, n, numPartitions=partitions or spark.sparkContext.defaultParallelism)

    # Build columns with vectorized expressions (no Python loops/UDFs)
    name_id_col:Column = F.col("id").cast("int")
    age_col: Column = (
        F.floor(F.rand(42) * (10 + 1)).cast("int") 
    )

    return df.select(
        name_id_col.alias("name_id"),
        F.concat(F.lit("name_"), F.col("id")).alias("name"),
        age_col.alias("age"),
        F.concat(F.lit("city_"), F.col("id")).alias("city"),
    )

    
    
def test_dq_manager(spark_session: SparkSession) -> None:
    dq_manager = DQManager(spark=spark_session)
    df: DataFrame = create_dataframe_fast(spark_session)
    df.show(10)
    print(df.count())
    dq_manager.set_data(df)
    dq_manager.add_check(CompletenessColRatioRule(
        dataset="test",
        run_id="test",
        severity_level=SeverityLevel.HIGH,
        metric_name="test",
        input_attributes=["name", "age", "city"],
        threshold=0.5)
    )
    result_obj: ResultObj = dq_manager.run()
    valid_df: DataFrame = result_obj.get_valid_df()
    valid_df.show(10) 
    invalid_df: DataFrame = result_obj.get_invalid_df()
    invalid_df.show(10) 
    metric_results: DataFrame = result_obj.get_metric_results()
    metric_results.show(10)