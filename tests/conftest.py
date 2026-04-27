import pytest
import os
from pyspark.sql import SparkSession
import tempfile
import shutil


@pytest.fixture(scope="session")
def spark():
    """
    Provide a shared SparkSession to tests and stop it when the test session ends.

    Returns:
        pyspark.sql.SparkSession: A SparkSession configured with master "local[1]" and app name "pytest-pyspark-local"; the session is stopped during fixture teardown.
    """
    # Set JAVA_HOME to the correct path
    os.environ["JAVA_HOME"] = os.path.expanduser(
        "~/.local/share/mise/installs/java/openjdk-17"
    )

    ICEBERG_VERSION = "1.10.1"
    SPARK_VERSION = "3.5_2.12"
    ICEBERG_PACKAGE = f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}:{ICEBERG_VERSION}"

    # Create warehouse directory
    warehouse_dir = tempfile.mkdtemp(prefix="iceberg_test_")

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("pytest-pyspark-local")
        # ADD THIS LINE:
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_dir)
        .getOrCreate()
    )

    yield spark
    spark.stop()
    # Clean up the temporary warehouse directory
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir)
