import pytest
import os
import tempfile
import shutil
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # 1. Kill any existing sessions
    if SparkSession.getActiveSession():
        SparkSession.getActiveSession().stop()

    # 2. Setup Environment (Ensure Java 17 and a supported Python are used)
    os.environ["JAVA_HOME"] = os.path.expanduser(
        "~/.local/share/mise/installs/java/temurin-17"
    )

    warehouse_dir = tempfile.mkdtemp(prefix="iceberg_warehouse_")
    # Absolute URI is mandatory for Hadoop catalogs on macOS/Linux
    warehouse_uri = f"file://{os.path.abspath(warehouse_dir)}"

    # 3. Spark 3.5 + Iceberg 1.10.1 Coordinates
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

    spark = builder.getOrCreate()
    yield spark
    spark.stop()
    shutil.rmtree(warehouse_dir, ignore_errors=True)
