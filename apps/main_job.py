import sys
import os

# Ensure src is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import (
    current_timestamp,
    row_number,
    monotonically_increasing_id,
)
from pyspark.sql.window import Window
from src.utils import get_spark_session
from src.transformations import clean_text_data, EXPECTED_SCHEMA


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
        window_spec = Window.partitionBy("id").orderBy(monotonically_increasing_id())
        deduped_updates = (
            cleaned_df.withColumn("rn", row_number().over(window_spec))
            .filter("rn = 1")
            .drop("rn")
        )
        deduped_updates.createOrReplaceTempView("updates")

        # Schema evolution: detect and apply changes before CREATE TABLE
        table_exists = spark.catalog.tableExists("local.db.silver_users")
        if table_exists:
            existing_table = spark.table("local.db.silver_users")
            existing_schema = existing_table.schema
            new_schema = cleaned_df.schema

            # Check for new or modified fields
            existing_fields = {field.name: field for field in existing_schema.fields}
            new_fields = {field.name: field for field in new_schema.fields}

            # (1) Detect dropped columns
            for field_name, existing_field in existing_fields.items():
                if field_name not in new_fields:
                    # Column removal detected - raise error
                    raise ValueError(
                        f"Column '{field_name}' exists in table but is missing from new schema. "
                        f"Explicit column removal is not supported."
                    )

            # (2) Check for new or modified fields with proper type/nullability comparison
            for field_name, field in new_fields.items():
                if field_name not in existing_fields:
                    # New column detected - add it
                    spark.sql(f"""
                        ALTER TABLE local.db.silver_users
                        ADD COLUMN `{field_name.replace("`", "")}` {field.dataType.simpleString()}
                    """)
                else:
                    existing_field = existing_fields[field_name]
                    # Compare types using simpleString() instead of object equality
                    if (
                        existing_field.dataType.simpleString()
                        != field.dataType.simpleString()
                    ):
                        # Type change detected - fail fast
                        raise ValueError(
                            f"Incompatible schema change for column '{field_name}': "
                            f"existing type {existing_field.dataType.simpleString()} "
                            f"cannot be changed to {field.dataType.simpleString()}"
                        )
                    # Check nullable differences
                    if existing_field.nullable and not field.nullable:
                        # Going from nullable to non-nullable requires explicit handling
                        raise ValueError(
                            f"Incompatible nullability change for column '{field_name}': "
                            f"cannot change from nullable to non-nullable without explicit ALTER"
                        )
                    # Allow making a column more nullable (non-nullable -> nullable) silently

        # Create silver_users table if not exists
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.silver_users (
                id INT,
                name STRING,
                status STRING,
                updated_at TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (status)
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
        # Only stop the session if it was created internally
        if not session_was_provided:
            spark.stop()


if __name__ == "__main__":
    main()
