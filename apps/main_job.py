import sys
import os

# Ensure src is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import (
    current_timestamp,
    row_number,
    monotonically_increasing_id,
    desc,
)
from pyspark.sql.window import Window
from src.utils import get_spark_session
from src.transformations import clean_text_data, EXPECTED_SCHEMA, upsert_to_silver


def main(session=None, csv_path="/opt/spark/project/data/input/sample.csv"):
    """
    Run the ETL pipeline that ingests raw CSV data, cleans and upserts user records, and writes aggregated user statistics.

    Performs three stages:
    - Bronze: reads input CSV and appends raw rows to the Iceberg table `local.db.bronze_events`.
    - Silver: cleans the `name` field, exposes cleaned rows as a temporary view `updates`, and merges (upserts) into `local.db.silver_users` keyed by `id`.
    - Gold: aggregates user counts by `status` from `local.db.silver_users` and replaces the contents of `local.db.gold_user_stats`.

    Args:
        session: Optional SparkSession. If None, creates a new session via get_spark_session().
        csv_path: Path to the input CSV file. Defaults to "/opt/spark/project/data/input/sample.csv".

    Side effects:
    - Reads from csv_path.
    - Writes/appends to Iceberg tables: `local.db.bronze_events`, `local.db.silver_users`, and `local.db.gold_user_stats`.
    """
    # Use provided session or create a new one
    session_was_provided = session is not None
    if session is None:
        session = get_spark_session()
    spark = session

    try:
        # Create namespace if not exists
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

        # 1. BRONZE: Raw Ingestion (Append-only)
        raw_df = spark.read.schema(EXPECTED_SCHEMA).csv(csv_path)

        # Create bronze_events table if not exists
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS local.db.bronze_events (
                {", ".join([f"`{field.name.replace('`', '')}` {field.dataType.simpleString()}" for field in EXPECTED_SCHEMA.fields])}
            ) USING iceberg
        """)

        raw_df.writeTo("local.db.bronze_events").append()

        # 2. SILVER: Cleaned & Validated (Merge/Upsert)
        cleaned_df = clean_text_data(raw_df, "name")
        # Add updated_at column to match canonical silver_users schema
        cleaned_df = cleaned_df.withColumn("updated_at", current_timestamp())

        # Deduplicate the source before merge to avoid ambiguous merge-key duplicates
        # Keep the latest row per id (using monotonically_increasing_id as tie-breaker)
        window_spec = Window.partitionBy("id").orderBy(
            desc(monotonically_increasing_id())
        )
        deduped_updates = (
            cleaned_df.withColumn("rn", row_number().over(window_spec))
            .filter("rn = 1")
            .drop("rn")
        )
        # Materialize the DataFrame to remove non-deterministic expressions
        # Collect and recreate DataFrame to break lineage with window functions
        deduped_updates = spark.createDataFrame(
            deduped_updates.collect(), deduped_updates.schema
        )

        # Use upsert_to_silver helper to handle table creation and merge
        upsert_to_silver(
            spark, deduped_updates, "local.db.silver_users", partition_spec="status"
        )

        # 3. GOLD: Aggregated for BI
        gold_df = spark.sql(
            "SELECT status, count(*) AS user_count FROM local.db.silver_users GROUP BY status"
        )
        gold_df.writeTo("local.db.gold_user_stats").createOrReplace()
    finally:
        # Only stop the session if it was created internally
        if not session_was_provided:
            spark.stop()


if __name__ == "__main__":
    main()
