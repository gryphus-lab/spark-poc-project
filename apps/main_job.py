import sys
import os

# Ensure src is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils import get_spark_session
from src.transformations import clean_text_data, EXPECTED_SCHEMA


def main():
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
