import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-local") \
        .getOrCreate()
    yield spark
    spark.stop()
