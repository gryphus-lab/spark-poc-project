import pytest
from apps import main_job
from src.transformations import EXPECTED_SCHEMA, upsert_to_silver


def test_debug_spark_env(spark):
    # Test basic Spark configuration and catalog access
    """
    Verify the Spark session has basic configuration and at least one available catalog.

    Performs assertions that Spark configuration is non-empty, that the `spark.app.name` setting is present, and that `SHOW CATALOGS` returns at least one catalog.

    Parameters:
        spark (pyspark.sql.SparkSession): Spark session used for the test.
    """
    conf = spark.sparkContext.getConf().getAll()
    assert len(conf) > 0, "Spark configuration should not be empty"
    conf_dict = dict(conf)
    assert "spark.app.name" in conf_dict, "Spark app name should be configured"

    # Test catalog access
    catalogs_df = spark.sql("SHOW CATALOGS")
    assert catalogs_df.count() > 0, "At least one catalog should be available"


def _ensure_iceberg_available(spark):
    """
    Ensure the Iceberg catalog "local.db" is available and selected for subsequent operations.

    Creates the namespace `local.db` if it does not exist and sets it as the current catalog/namespace using the provided Spark session. If catalog initialization fails, the test is failed via pytest.fail with any available Java exception detail.

    Parameters:
        spark (pyspark.sql.SparkSession): Spark session used to execute the catalog DDL.
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


def test_upsert_to_silver_merges_inserts_and_updates(spark):
    _ensure_iceberg_available(spark)
    table_name = "local.db.integration_silver_upsert"

    try:
        # Initial Insert
        initial_rows = [(1, "Alice", "Active"), (2, "Bob", "Inactive")]
        initial_df = spark.createDataFrame(initial_rows, EXPECTED_SCHEMA)
        upsert_to_silver(spark, initial_df, table_name, partition_spec="status")

        assert spark.catalog.tableExists(table_name)

        # Verify initial state
        results = {
            row["id"]: (row["name"], row["status"])
            for row in spark.table(table_name).collect()
        }
        assert results[1] == ("Alice", "Active")
        assert results[2] == ("Bob", "Inactive")

        # Perform Upsert (Update existing + Insert new)
        updated_rows = [(1, "Alice Updated", "Active"), (3, "Carol", "Active")]
        updated_df = spark.createDataFrame(updated_rows, EXPECTED_SCHEMA)
        upsert_to_silver(spark, updated_df, table_name, partition_spec="status")

        final_results = {
            row["id"]: (row["name"], row["status"])
            for row in spark.table(table_name).collect()
        }
        assert len(final_results) == 3
        assert final_results[1] == ("Alice Updated", "Active")
        assert final_results[2] == ("Bob", "Inactive")  # Untouched record preserved
        assert final_results[3] == ("Carol", "Active")

    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_main_job_etl_flow_creates_iceberg_tables_and_aggregates(spark, monkeypatch):
    """
    Run the ETL entrypoint with a mocked CSV input and assert that Iceberg tables are created and gold-layer aggregations are correct.

    Mocks pyspark.sql.DataFrameReader.csv to return a small sample DataFrame, invokes main_job.main(session=spark), then asserts that the expected Iceberg tables (local.db.bronze_events, local.db.silver_users, local.db.gold_user_stats) exist and that the gold_user_stats table contains one "Active" and one "Inactive" user count. Cleans up created tables on completion.

    Parameters:
        spark (pyspark.sql.SparkSession): Pytest Spark session fixture used to run the ETL job.
        monkeypatch (pytest.MonkeyPatch): Pytest monkeypatch fixture used to stub the CSV reader.
    """
    _ensure_iceberg_available(spark)

    sample_rows = [
        (1, "ALICE", "Active"),
        (2, "BOB", "Inactive"),
        (2, "BOB UPDATED", "Inactive"),
    ]
    sample_df = spark.createDataFrame(sample_rows, EXPECTED_SCHEMA)

    # Mock the CSV reader to return our sample DataFrame
    import pyspark.sql

    monkeypatch.setattr(
        pyspark.sql.DataFrameReader,
        "csv",
        lambda self, _path, *_args, **_kwargs: sample_df,
    )

    try:
        main_job.main(session=spark)

        # Verify all table tiers were created
        assert spark.catalog.tableExists("local.db.bronze_events")
        assert spark.catalog.tableExists("local.db.silver_users")
        assert spark.catalog.tableExists("local.db.gold_user_stats")

        # Validate Gold layer logic
        gold_rows = spark.table("local.db.gold_user_stats").collect()
        status_counts = {row["status"]: row["user_count"] for row in gold_rows}
        assert status_counts["Active"] == 1
        assert status_counts["Inactive"] == 1

    finally:
        # Ensure cleanup of all test tables
        for table in ["bronze_events", "silver_users", "gold_user_stats"]:
            spark.sql(f"DROP TABLE IF EXISTS local.db.{table}")
