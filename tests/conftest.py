import pytest
import os
import sys
import tempfile
import shutil
from pyspark.sql import SparkSession


def _ensure_iceberg_available(spark):
    """
    Forces initialization of the Iceberg catalog by performing a
    namespace operation using the fully-qualified name.
    """
    try:
        # Trigger catalog load by referencing it directly in a DDL command
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
        # Once created, we can switch safely
        spark.sql("USE local.db")
    except Exception as e:
        # Catch and report the specific Java cause
        error_detail = str(e)
        if hasattr(e, "java_exception"):
            error_detail = str(e.java_exception)
        pytest.fail(f"Iceberg Catalog Setup Failed: {error_detail}")


@pytest.fixture(scope="session")
def spark():
    # 1. Kill any existing sessions
    session = SparkSession.getActiveSession()
    if session:
        session.stop()

    # 2. Setup Environment (Ensure Java 17 and a supported Python are used)
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        java_home = os.path.expanduser("~/.local/share/mise/installs/java/temurin-17")
        if not os.path.exists(java_home):
            raise EnvironmentError(
                f"JAVA_HOME not set and fallback path {java_home} does not exist"
            )
    os.environ["JAVA_HOME"] = java_home

    # 3. Set PYSPARK environment variables to use the correct Python version
    # This ensures Spark executors use the same Python version as the driver
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    warehouse_dir = tempfile.mkdtemp(prefix="iceberg_warehouse_")
    # Absolute URI is mandatory for Hadoop catalogs on macOS/Linux
    warehouse_uri = f"file://{os.path.abspath(warehouse_dir)}"

    # 4. Spark 3.5 + Iceberg 1.10.1 Coordinates
    ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("pytest-iceberg")
        .config("spark.jars.packages", ICEBERG_PKG)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_uri)
        # Fix Java 17 reflection for Spark internals
        .config(
            "spark.driver.extraJavaOptions",
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.net=ALL-UNNAMED "
            "--add-opens=java.base/java.io=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "-Dio.netty.tryReflectionSetAccessible=true",
        )
    )

    try:
        spark = builder.getOrCreate()
    except Exception:
        shutil.rmtree(warehouse_dir, ignore_errors=True)
        raise

    yield spark
    spark.stop()
    shutil.rmtree(warehouse_dir, ignore_errors=True)
