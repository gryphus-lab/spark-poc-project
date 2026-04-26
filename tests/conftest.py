import pytest
import os
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Provide a shared SparkSession to tests and stop it when the test session ends.

    Returns:
        pyspark.sql.SparkSession: A SparkSession configured with master "local[1]" and app name "pytest-pyspark-local"; the session is stopped during fixture teardown.
    """
    # Set JAVA_HOME to the correct path
    os.environ["JAVA_HOME"] = os.path.expanduser(
        "~/.local/share/mise/installs/java/openjdk-25"
    )

    # Create warehouse directory
    import tempfile
    warehouse_dir = tempfile.mkdtemp(prefix="iceberg_test_")
    os.environ["SPARK_LOCAL_DIRS"] = warehouse_dir

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("pytest-pyspark-local")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_dir)
        .getOrCreate()
    )
    yield spark
    spark.stop()
