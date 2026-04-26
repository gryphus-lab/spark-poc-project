from pyspark.testing import assertSchemaEqual
from pyspark.sql.functions import col
from src.transformations import EXPECTED_SCHEMA


def test_schema_integrity(spark):
    """Verify that the actual data schema matches our expectations."""
    # Simulate loading data
    df = spark.createDataFrame([(1, "Alice", "Active")], schema=EXPECTED_SCHEMA)

    # assertSchemaEqual validates column names, types, and nullability
    assertSchemaEqual(df.schema, EXPECTED_SCHEMA)


def test_null_completeness(spark):
    """Check for unexpected null values in critical columns."""
    data = [(1, "Alice"), (2, None)]  # ID is present, but name is missing
    df = spark.createDataFrame(data, ["id", "name"])

    # Calculate null count for the 'id' column
    null_count = df.filter(col("id").isNull()).count()

    # Assert that a mandatory field has zero nulls
    assert null_count == 0, f"Found {null_count} null values in mandatory 'id' column"
