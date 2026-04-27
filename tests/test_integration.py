import pytest
from apps import main_job
from src.transformations import EXPECTED_SCHEMA, upsert_to_silver

import os


def test_debug_spark_env(spark):
    print("\n--- SPARK DEBUG INFO ---")

    # 1. Print all Spark Configurations
    conf = spark.sparkContext.getConf().getAll()
    print("Active Configurations:")
    for k, v in sorted(conf):
        print(f"  {k}: {v}")

    # 2. Check for loaded Iceberg Jars in the JVM
    print("\nLoaded Jars in JVM:")
    try:
        # Access the internal Java Spark Context to list jars
        jvm_jars = spark.sparkContext._jsc.sc().listJars()
        # Convert Java Collection to Python list
        jars_list = [jvm_jars.apply(i) for i in range(jvm_jars.length())]
        for jar in jars_list:
            print(f"  Found Jar: {jar}")
        if not jars_list:
            print("  No external Jars detected in JVM.")
    except Exception as e:
        print(f"  Could not list Jars: {e}")

    # 3. Check Java Runtime

    print(f"\nEnvironment JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not Set')}")

    # 4. Test basic Catalog access
    print("\nCatalog Status:")
    try:
        spark.sql("SHOW CATALOGS").show()
    except Exception as e:
        print(f"  SHOW CATALOGS failed: {e}")


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
        error_detail = getattr(e, "desc", str(e))
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
        assert final_results[3] == ("Carol", "Active")

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

    # Mock the CSV reader to return our sample DataFrame
    import pyspark.sql

    monkeypatch.setattr(
        pyspark.sql.DataFrameReader, "csv", lambda self, path: sample_df
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
