import os
from pyspark.sql import SparkSession


def get_spark_session(app_name="IcebergDataLake"):
    """
    Create a SparkSession configured for Apache Iceberg, a REST-backed `local` catalog, and S3A (MinIO) access.

    Parameters:
        app_name (str): Application name to set for the Spark session. Defaults to "IcebergDataLake".

    Returns:
        SparkSession: A SparkSession configured with Iceberg Spark extensions, a `local` REST catalog pointing at http://catalog:8181, and S3A/MinIO endpoint and credentials.
    """
    s3a_access_key = os.environ.get("S3A_ACCESS_KEY", "admin")
    s3a_secret_key = os.environ.get("S3A_SECRET_KEY", "password")

    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "rest")
        .config("spark.sql.catalog.local.uri", "http://catalog:8181")
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.local.s3.access-key", s3a_access_key)
        .config("spark.sql.catalog.local.s3.secret-key", s3a_secret_key)
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", s3a_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3a_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
