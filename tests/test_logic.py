from src.transformations import clean_text_data, upsert_to_silver
from src.utils import get_spark_session
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
import pytest


def test_clean_text_data(spark):
    # 1. Arrange: Create a sample DataFrame
    input_data = [("APACHE SPARK",), ("PySpark",), ("Docker",)]
    df = spark.createDataFrame(input_data, ["name"])

    # 2. Act: Apply the transformation
    result_df = clean_text_data(df, "name")

    # 3. Assert: Check if all values are now lowercase
    results = [row["name"] for row in result_df.collect()]

    assert "apache spark" in results
    assert "pyspark" in results
    assert "docker" in results
    assert "APACHE SPARK" not in results
    assert "PySpark" not in results
    assert "Docker" not in results


def test_get_spark_session():
    # Act: Call the function
    session = get_spark_session()

    # Assert: Check that it returns a SparkSession
    assert isinstance(session, SparkSession)


def test_clean_text_data_empty_string(spark):
    """Empty strings remain empty after lowercasing."""
    df = spark.createDataFrame([("",)], ["name"])
    result_df = clean_text_data(df, "name")
    results = [row["name"] for row in result_df.collect()]
    assert results == [""]


def test_clean_text_data_already_lowercase(spark):
    """Values that are already lowercase are unchanged."""
    df = spark.createDataFrame([("already lower",), ("hello world",)], ["name"])
    result_df = clean_text_data(df, "name")
    results = [row["name"] for row in result_df.collect()]
    assert "already lower" in results
    assert "hello world" in results


def test_clean_text_data_null_values(spark):
    """Null values in the target column remain null after transformation."""
    schema = StructType([StructField("name", StringType(), nullable=True)])
    df = spark.createDataFrame([(None,)], schema=schema)
    result_df = clean_text_data(df, "name")
    results = [row["name"] for row in result_df.collect()]
    assert results == [None]


def test_clean_text_data_mixed_null_and_values(spark):
    """Mix of null and non-null values are each handled correctly."""
    schema = StructType([StructField("name", StringType(), nullable=True)])
    df = spark.createDataFrame([("UPPER",), (None,), ("Mixed",)], schema=schema)
    result_df = clean_text_data(df, "name")
    results = [row["name"] for row in result_df.collect()]
    assert "upper" in results
    assert None in results
    assert "mixed" in results


def test_clean_text_data_special_characters(spark):
    """Special characters and punctuation are preserved during lowercasing."""
    df = spark.createDataFrame([("Hello, World!",), ("TEST-CASE_123",)], ["name"])
    result_df = clean_text_data(df, "name")
    results = [row["name"] for row in result_df.collect()]
    assert "hello, world!" in results
    assert "test-case_123" in results


def test_clean_text_data_numbers_in_string(spark):
    """Strings containing numbers lowercase letters and leave digits unchanged."""
    df = spark.createDataFrame([("ABC123",), ("Version2.0",)], ["name"])
    result_df = clean_text_data(df, "name")
    results = [row["name"] for row in result_df.collect()]
    assert "abc123" in results
    assert "version2.0" in results


def test_clean_text_data_preserves_other_columns(spark):
    """Only the target column is modified; other columns are left untouched."""
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
        ]
    )
    df = spark.createDataFrame([(1, "ALICE"), (2, "BOB")], schema=schema)
    result_df = clean_text_data(df, "name")
    rows = {row["id"]: row["name"] for row in result_df.collect()}
    assert rows[1] == "alice"


@pytest.mark.xfail(strict=True, reason="Iceberg not available in test environment")
def test_upsert_to_silver_insert_only(spark):
    """Test upsert_to_silver function for insert-only case."""
    from pyspark.sql.functions import current_timestamp

    # Create namespace if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

    # Create new data
    new_data = [(1, "Alice", "Active"), (2, "Bob", "Inactive")]
    df = spark.createDataFrame(new_data, ["id", "name", "status"]).withColumn("updated_at", current_timestamp())

    # Use a temporary table name for testing
    table_name = "local.db.test_silver_table_insert"

    # Call the function
    upsert_to_silver(spark, df, table_name, partition_spec="status")

    # Verify the table was created and data inserted
    assert spark.catalog.tableExists(table_name)
    result_df = spark.table(table_name)
    rows = result_df.collect()
    assert len(rows) == 2

    # Check that data matches
    row_dict = {row["id"]: (row["name"], row["status"]) for row in rows}
    assert row_dict[1] == ("Alice", "Active")
    assert row_dict[2] == ("Bob", "Inactive")

    # Clean up
    spark.sql(f"DROP TABLE {table_name}")


def test_clean_text_data_different_column_name(spark):
    """clean_text_data works correctly when applied to a column other than 'name'."""
    df = spark.createDataFrame([("ACTIVE",), ("INACTIVE",)], ["status"])
    result_df = clean_text_data(df, "status")
    results = [row["status"] for row in result_df.collect()]
    assert "active" in results
    assert "inactive" in results
    assert "ACTIVE" not in results


def test_clean_text_data_row_count_unchanged(spark):
    """The number of rows in the DataFrame is unchanged after the transformation."""
    input_data = [("A",), ("B",), ("C",), ("D",), ("E",)]
    df = spark.createDataFrame(input_data, ["name"])
    result_df = clean_text_data(df, "name")
    assert result_df.count() == len(input_data)


def test_get_spark_session_with_custom_app_name(spark):
    """get_spark_session accepts a custom app name and returns a SparkSession."""
    session = get_spark_session("CustomTestApp")
    assert isinstance(session, SparkSession)
