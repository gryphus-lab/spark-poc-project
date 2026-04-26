import pytest
from py4j.protocol import Py4JJavaError

from apps import main_job
from src.transformations import EXPECTED_SCHEMA, upsert_to_silver


def _ensure_iceberg_available(spark):
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    except Py4JJavaError:
        pytest.skip("Iceberg catalog is not available in this environment")


def test_upsert_to_silver_merges_inserts_and_updates(spark):
    _ensure_iceberg_available(spark)
    table_name = "local.db.integration_silver_upsert"

    try:
        # Insert initial rows into the target Iceberg table.
        initial_rows = [(1, "Alice", "Active"), (2, "Bob", "Inactive")]
        initial_df = spark.createDataFrame(initial_rows, EXPECTED_SCHEMA)
        upsert_to_silver(spark, initial_df, table_name, partition_spec="status")

        assert spark.catalog.tableExists(table_name)
        rows = {row["id"]: (row["name"], row["status"]) for row in spark.table(table_name).collect()}
        assert rows[1] == ("Alice", "Active")
        assert rows[2] == ("Bob", "Inactive")

        # Upsert a changed row and a new row.
        updated_rows = [(1, "Alice Updated", "Active"), (3, "Carol", "Active")]
        updated_df = spark.createDataFrame(updated_rows, EXPECTED_SCHEMA)
        upsert_to_silver(spark, updated_df, table_name, partition_spec="status")

        final_rows = {row["id"]: (row["name"], row["status"]) for row in spark.table(table_name).collect()}
        assert len(final_rows) == 3
        assert final_rows[1] == ("Alice Updated", "Active")
        assert final_rows[3] == ("Carol", "Active")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_main_job_etl_flow_creates_iceberg_tables_and_aggregates(spark, monkeypatch):
    _ensure_iceberg_available(spark)
    sample_rows = [
        (1, "ALICE", "Active"),
        (2, "BOB", "Inactive"),
        (2, "BOB UPDATED", "Inactive"),
    ]
    sample_df = spark.createDataFrame(sample_rows, EXPECTED_SCHEMA)

    # Return the pytest SparkSession and override the CSV read to use sample data.
    monkeypatch.setattr(main_job, "get_spark_session", lambda: spark)
    monkeypatch.setattr(spark.read, "csv", lambda path: sample_df)

    try:
        main_job.main()

        assert spark.catalog.tableExists("local.db.bronze_events")
        assert spark.catalog.tableExists("local.db.silver_users")
        assert spark.catalog.tableExists("local.db.gold_user_stats")

        bronze_count = spark.table("local.db.bronze_events").count()
        silver_rows = spark.table("local.db.silver_users").collect()
        gold_rows = spark.table("local.db.gold_user_stats").collect()

        assert bronze_count == 3
        assert len(silver_rows) == 2

        status_counts = {row["status"]: row["user_count"] for row in gold_rows}
        assert status_counts["Active"] == 1
        assert status_counts["Inactive"] == 1

        cleaned_names = {row["id"]: row["name"] for row in silver_rows}
        assert cleaned_names[1] == "alice"
        assert cleaned_names[2] == "bob updated"
    finally:
        spark.sql("DROP TABLE IF EXISTS local.db.bronze_events")
        spark.sql("DROP TABLE IF EXISTS local.db.silver_users")
        spark.sql("DROP TABLE IF EXISTS local.db.gold_user_stats")
