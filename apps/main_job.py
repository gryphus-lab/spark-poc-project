import sys
import os

# Ensure src is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils import get_spark_session
from src.transformations import clean_text_data, EXPECTED_SCHEMA


def main():
    """
    Run the ETL pipeline that ingests raw CSV data, cleans and upserts user records, and writes aggregated user statistics.
    
    Performs three stages:
    - Bronze: reads input CSV and appends raw rows to the Iceberg table `local.db.bronze_events`.
    - Silver: cleans the `name` field, exposes cleaned rows as a temporary view `updates`, and merges (upserts) into `local.db.silver_users` keyed by `id`.
    - Gold: aggregates user counts by `status` from `local.db.silver_users` and replaces the contents of `local.db.gold_user_stats`.
    
    Side effects:
    - Reads from "/opt/spark/project/data/input/sample.csv".
    - Writes/appends to Iceberg tables: `local.db.bronze_events`, `local.db.silver_users`, and `local.db.gold_user_stats`.
    """
    spark = get_spark_session()

    try:
        # Create namespace if not exists
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

        # 1. BRONZE: Raw Ingestion (Append-only)
        raw_df = spark.read.schema(EXPECTED_SCHEMA).csv(
            "/opt/spark/project/data/input/sample.csv"
        )

        # Create bronze_events table if not exists
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS local.db.bronze_events (
                {", ".join([f"{field.name} {field.dataType.simpleString()}" for field in EXPECTED_SCHEMA.fields])}
            ) USING iceberg
        """)

        raw_df.writeTo("local.db.bronze_events").append()

        # 2. SILVER: Cleaned & Validated (Merge/Upsert)
        cleaned_df = clean_text_data(raw_df, "name")
        cleaned_df.createOrReplaceTempView("updates")

        # Create silver_users table if not exists
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS local.db.silver_users (
                {", ".join([f"{field.name} {field.dataType.simpleString()}" for field in cleaned_df.schema.fields])}
            ) USING iceberg
        """)

        # Iceberg supports SQL Merge (upsert logic)
        spark.sql("""
            MERGE INTO local.db.silver_users t
            USING updates s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        # 3. GOLD: Aggregated for BI
        gold_df = spark.sql(
            "SELECT status, count(*) AS user_count FROM local.db.silver_users GROUP BY status"
        )
        gold_df.writeTo("local.db.gold_user_stats").createOrReplace()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
