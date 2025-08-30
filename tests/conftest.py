from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    return SparkSession.builder.appName("DQTask").getOrCreate() # type: ignore