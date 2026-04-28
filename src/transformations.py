from pyspark.sql.functions import col, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from uuid import uuid4
import re
import logging

logger = logging.getLogger(__name__)


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
    """
    Retrieve the Iceberg table's partition column names from its metadata.

    Parameters:
        table_name (str): Table identifier to describe (for example 'db.table' or 'catalog.db.table').

    Returns:
        list[str]: Ordered list of partition column names. Returns an empty list if the table does not exist or its partition metadata cannot be read.
    """
    try:
        # Parse partition information from DESCRIBE EXTENDED
        rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
        partitions = []
        in_section = False

        for row in rows:
            col_name = (row["col_name"] or "").strip()
            if col_name == "# Partition Information":
                in_section = True
                continue
            if in_section:
                if col_name.startswith("#"):
                    break
                if col_name:
                    partitions.append(col_name)
        return partitions
    except Exception as e:
        # Return empty list if table doesn't exist or metadata can't be read
        logger.debug(f"Failed to retrieve partitions for {table_name}: {e}")
        return []


def _generate_schema_ddl(df):
    """Derives DDL schema string from DataFrame schema."""
    return ", ".join(
        [
            f"`{f.name.replace('`', '')}` {f.dataType.simpleString()}"
            for f in df.schema.fields
        ]
    )


def _validate_table_name(table_name):
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", table_name):
        raise ValueError(f"Invalid table_name: {table_name}")


def _validate_partition_spec(partition_spec):
    for p in partition_spec.split(","):
        p = p.strip()
        if not re.match(r"^[a-zA-Z_][\w]*$", p):
            raise ValueError(f"Invalid partition identifier: {p}")


def _validate_merge_key(df, merge_key):
    if merge_key not in df.columns:
        raise ValueError(f"Merge key '{merge_key}' not found in columns: {df.columns}")


def _validate_existing_partitions(spark, table_name, partition_spec):
    if not partition_spec or not spark.catalog.tableExists(table_name):
        return
    existing = _get_existing_partitions(spark, table_name)
    requested = [p.strip() for p in partition_spec.split(",")]
    if existing and set(existing) != set(requested):
        raise ValueError(f"Partition mismatch for {table_name}. Existing: {existing}")


def _build_merge_clauses(df, merge_key):
    cols_wrapped = [f"`{c.replace('`', '')}`" for c in df.columns]
    m_key_wrapped = f"`{merge_key.replace('`', '')}`"
    update_set = ", ".join(
        [
            f"t.`{c.replace('`', '')}` = s.`{c.replace('`', '')}`"
            for c in df.columns
            if c != merge_key
        ]
    )
    update_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}" if update_set else ""
    return cols_wrapped, m_key_wrapped, update_clause


def upsert_to_silver(
    spark, df, table_name, partition_spec=None, table_schema=None, merge_key="id"
):
    # Validate identifiers to prevent SQL injection
    """
    Create (if needed) an Iceberg table and upsert rows from a DataFrame into it using a MERGE.

    Parameters:
        spark: SparkSession used to execute SQL and manage temporary views.
        df: DataFrame containing rows to be merged into the target table.
        table_name (str): Fully qualified table identifier (dots allowed for namespace).
        partition_spec (str, optional): Comma-separated partition column names to use for table creation.
        table_schema (str, optional): SQL column definitions (DDL) to use when creating the table; if omitted, schema is derived from df.
        merge_key (str, optional): Column name used as the merge key to match existing rows; defaults to "id".

    Raises:
        ValueError: If table_name or any partition identifier is invalid, if merge_key is not present in df.columns, or if an existing table's partitioning conflicts with the requested partition_spec.
    """
    _validate_table_name(table_name)
    if partition_spec:
        _validate_partition_spec(partition_spec)

    temp_view = f"incoming_{table_name.replace('.', '_')}_{uuid4().hex}"
    df.createOrReplaceTempView(temp_view)

    try:
        schema_ddl = table_schema or _generate_schema_ddl(df)
        partition_clause = (
            f"PARTITIONED BY ({partition_spec})" if partition_spec else ""
        )

        _validate_existing_partitions(spark, table_name, partition_spec)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_ddl}) USING iceberg {partition_clause}"
        )

        _validate_merge_key(df, merge_key)
        cols_wrapped, m_key_wrapped, update_clause = _build_merge_clauses(df, merge_key)

        spark.sql(f"""
            MERGE INTO {table_name} t USING {temp_view} s ON t.{m_key_wrapped} = s.{m_key_wrapped}
            {update_clause}
            WHEN NOT MATCHED THEN INSERT ({", ".join(cols_wrapped)}) VALUES ({", ".join([f"s.{c}" for c in cols_wrapped])})
        """)
    finally:
        spark.catalog.dropTempView(temp_view)
