import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Provide a shared SparkSession to tests and stop it when the test session ends.
    
    Returns:
        pyspark.sql.SparkSession: A SparkSession configured with master "local[1]" and app name "pytest-pyspark-local"; the session is stopped during fixture teardown.
    """
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-local") \
        .getOrCreate()
    yield spark
    spark.stop()
