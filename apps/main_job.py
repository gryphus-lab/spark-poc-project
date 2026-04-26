import sys
import os

# Ensure src is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils import get_spark_session
from src.transformations import clean_text_data, EXPECTED_SCHEMA


def main():
    """
    Run the ETL pipeline that ingests raw CSV data, cleans and upserts user records, and writes aggregated user statistics.

    This function performs three stages:
    - Bronze: reads /opt/spark/project/data/input/sample.csv using the expected schema and appends the raw rows to the Iceberg table local.db.bronze_events.
    - Silver: cleans text on the `name` field, registers the cleaned rows as a temporary view `updates`, and merges (upserts) those rows into the Iceberg table local.db.silver_users using `id` as the key.
    - Gold: computes counts of users grouped by `status` from local.db.silver_users and writes the result to the Iceberg table local.db.gold_user_stats (create or replace).

    Side effects:
    - Reads from the specified input CSV file.
    - Writes/appends to Iceberg tables: local.db.bronze_events, local.db.silver_users (via MERGE), and local.db.gold_user_stats.
    """
    spark = get_spark_session()

    # 1. BRONZE: Raw Ingestion (Append-only)
    raw_df = spark.read.schema(EXPECTED_SCHEMA).csv(
        "/opt/spark/project/data/input/sample.csv"
    )
    raw_df.writeTo("local.db.bronze_events").append()

    # 2. SILVER: Cleaned & Validated (Merge/Upsert)
    cleaned_df = clean_text_data(raw_df, "name")
    cleaned_df.createOrReplaceTempView("updates")

    # Iceberg supports SQL Merge (upsert logic)
    spark.sql("""
        MERGE INTO local.db.silver_users t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    # 3. GOLD: Aggregated for BI
    gold_df = spark.sql(
        "SELECT status, count(*) FROM local.db.silver_users GROUP BY status"
    )
    gold_df.writeTo("local.db.gold_user_stats").createOrReplace()


if __name__ == "__main__":
    main()
