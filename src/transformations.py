from pyspark.sql.functions import col, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from uuid import uuid4


EXPECTED_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)


def clean_text_data(df, column_name):
    return df.withColumn(column_name, lower(col(column_name)))


def _get_existing_partitions(spark, table_name):
    """Helper to parse Iceberg partition columns from table metadata."""
    try:
        # Parse partition information from DESCRIBE EXTENDED
        rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
        partitions = []
        in_section = False

        for row in rows:
            col = (row["col_name"] or "").strip()
            if col == "# Partition Information":
                in_section = True
                continue
            if in_section:
                if col.startswith("#"):
                    break
                if col:
                    partitions.append(col)
        return partitions
    except Exception:
        # Return empty list if table doesn't exist or metadata can't be read
        return []


def _generate_schema_ddl(df):
    """Derives DDL schema string from DataFrame schema."""
    return ", ".join(
        [
            f"`{f.name.replace('`', '')}` {f.dataType.simpleString()}"
            for f in df.schema.fields
        ]
    )


def upsert_to_silver(
    spark, df, table_name, partition_spec=None, table_schema=None, merge_key="id"
):
    # 1. Setup Temp View
    temp_view = f"incoming_{table_name.replace('.', '_')}_{uuid4().hex}"
    df.createOrReplaceTempView(temp_view)

    # 2. Handle Schema and Partitions
    schema_ddl = table_schema or _generate_schema_ddl(df)
    partition_clause = f"PARTITIONED BY ({partition_spec})" if partition_spec else ""

    # 3. Validate Existing Table Partitioning
    if partition_spec and spark.catalog.tableExists(table_name):
        existing = _get_existing_partitions(spark, table_name)
        requested = [p.strip() for p in partition_spec.split(",")]
        # Skip validation if we can't determine existing partitions (Iceberg compatibility)
        if existing and set(existing) != set(requested):
            raise ValueError(
                f"Partition mismatch for {table_name}. Existing: {existing}"
            )

    # 4. Create Table
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_ddl}) USING iceberg {partition_clause}"
    )

    # 5. Build Merge SQL
    cols = [f"`{c.replace('`', '')}`" for c in df.columns]
    m_key = f"`{merge_key.replace('`', '')}`"

    if m_key not in cols:
        raise ValueError(f"Merge key '{merge_key}' not found in columns: {df.columns}")

    update_set = ", ".join([f"t.{c} = s.{c}" for c in cols if c != m_key])
    update_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}" if update_set else ""

    spark.sql(f"""
        MERGE INTO {table_name} t USING {temp_view} s ON t.{m_key} = s.{m_key}
        {update_clause}
        WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join([f"s.{c}" for c in cols])})
    """)
