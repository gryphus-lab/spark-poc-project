import os
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark_session(app_name="IcebergDataLake"):
    """
    Create and configure a SparkSession for Apache Iceberg with a Hadoop 'local' catalog and S3A (MinIO) connectivity.

    Parameters:
        app_name (str): Name to assign to the Spark application. Defaults to "IcebergDataLake".

    Returns:
        SparkSession: A SparkSession configured with Iceberg Spark extensions, a `local` Hadoop catalog (warehouse at /opt/spark/warehouse), and S3A/MinIO endpoint and credentials.
    """
    # Detect non-local environment via common environment indicators
    is_non_local = (
        os.environ.get("ENV") in ("production", "staging", "prod", "stg")
        or os.environ.get("ENVIRONMENT") in ("production", "staging", "prod", "stg")
        or os.environ.get("AWS_EXECUTION_ENV") is not None
        or os.environ.get("KUBERNETES_SERVICE_HOST") is not None
    )

    s3a_access_key = os.environ.get("S3A_ACCESS_KEY")
    s3a_secret_key = os.environ.get("S3A_SECRET_KEY")

    if is_non_local:
        # In non-local environments, fail fast if credentials are missing
        if not s3a_access_key or not s3a_secret_key:
            raise ValueError(
                "S3A credentials (S3A_ACCESS_KEY and S3A_SECRET_KEY) are required in non-local environments. "
                "Please set both environment variables."
            )
    else:
        # In local/dev/test environments, use safe defaults
        if not s3a_access_key:
            s3a_access_key = "admin"
            logger.debug("S3A_ACCESS_KEY not set, using dev default 'admin'")
        if not s3a_secret_key:
            s3a_secret_key = "password"
            logger.debug("S3A_SECRET_KEY not set, using dev default 'password'")

    # Get S3 endpoint from environment with docker-compose default
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")

    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "/opt/spark/warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3a_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3a_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
