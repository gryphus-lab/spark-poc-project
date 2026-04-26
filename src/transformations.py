from pyspark.sql.functions import col, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def clean_text_data(df, column_name):
    return df.withColumn(column_name, lower(col(column_name)))


EXPECTED_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)


def upsert_to_silver(spark, df, table_name):
    """
    Upserts (Merges) data into an Iceberg Silver table.
    """
    # Register the incoming data as a temporary view
    df.createOrReplaceTempView("incoming_data")

    # Ensure the target table exists (Bronze-to-Silver initialization)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT,
            name STRING,
            status STRING,
            updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (status)
    """)

    # Perform the Merge logic
    spark.sql(f"""
        MERGE INTO {table_name} t
        USING incoming_data s
        ON t.id = s.id
        WHEN MATCHED THEN
            UPDATE SET t.name = s.name, t.status = s.status, t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT *
    """)
