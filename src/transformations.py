from pyspark.sql.functions import col, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from uuid import uuid4


def clean_text_data(df, column_name):
    return df.withColumn(column_name, lower(col(column_name)))


EXPECTED_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)


def upsert_to_silver(
    spark, df, table_name, partition_spec=None, table_schema=None, merge_key="id"
):
    """
    Upserts (Merges) data into an Iceberg Silver table.

    Args:
        spark: SparkSession
        df: DataFrame to upsert
        table_name: Fully qualified table name (e.g., 'local.db.silver_users')
        partition_spec: Optional partition specification (e.g., 'status').
                        If None, no partitioning is applied.
        table_schema: Optional table schema. If None, derives from df.schema.
        merge_key: Column name to use for MERGE matching (default "id").
    """
    # Create a unique temp view name to avoid collisions
    temp_view = f"incoming_{table_name.replace('.', '_')}_{uuid4().hex}"
    df.createOrReplaceTempView(temp_view)

    # Use provided schema or derive from DataFrame
    if table_schema is None:
        # Build schema from DataFrame columns
        schema_fields = []
        for field in df.schema.fields:
            schema_fields.append(
                f"`{field.name.replace('`', '')}` {field.dataType.simpleString()}"
            )
        schema_ddl = ", ".join(schema_fields)
    else:
        schema_ddl = table_schema

    # Build partition clause
    partition_clause = ""
    if partition_spec:
        partition_clause = f"PARTITIONED BY ({partition_spec})"

    # Ensure the target table exists (Bronze-to-Silver initialization)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema_ddl}
        ) USING iceberg
        {partition_clause}
    """)

    # Build explicit column lists for MERGE to fail early on mismatches
    columns = [f"`{col.replace('`', '')}`" for col in df.columns]

    # Validate merge_key exists in columns
    merge_key_escaped = f"`{merge_key.replace('`', '')}`"
    if merge_key_escaped not in columns:
        raise ValueError(
            f"Merge key '{merge_key}' not found in DataFrame columns: {df.columns}"
        )

    insert_cols = ", ".join(columns)
    insert_values = ", ".join([f"s.{col}" for col in columns])
    update_set = ", ".join(
        [f"t.{col} = s.{col}" for col in columns if col != merge_key_escaped]
    )

    # Perform the Merge logic with explicit column lists
    spark.sql(f"""
        MERGE INTO {table_name} t
        USING {temp_view} s
        ON t.{merge_key_escaped} = s.{merge_key_escaped}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_values})
    """)
