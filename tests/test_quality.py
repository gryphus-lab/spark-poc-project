from pyspark.testing import assertSchemaEqual
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType
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


# --- EXPECTED_SCHEMA structure tests ---


def test_expected_schema_field_count():
    """EXPECTED_SCHEMA must define exactly 3 fields."""
    assert len(EXPECTED_SCHEMA.fields) == 3


def test_expected_schema_field_names():
    """EXPECTED_SCHEMA must contain exactly the fields: id, name, status."""
    names = [f.name for f in EXPECTED_SCHEMA.fields]
    assert names == ["id", "name", "status"]


def test_expected_schema_id_type():
    """The 'id' field must be IntegerType."""
    id_field = EXPECTED_SCHEMA["id"]
    assert isinstance(id_field.dataType, IntegerType)


def test_expected_schema_name_type():
    """The 'name' field must be StringType."""
    name_field = EXPECTED_SCHEMA["name"]
    assert isinstance(name_field.dataType, StringType)


def test_expected_schema_status_type():
    """The 'status' field must be StringType."""
    status_field = EXPECTED_SCHEMA["status"]
    assert isinstance(status_field.dataType, StringType)


def test_expected_schema_id_not_nullable():
    """The 'id' field must not be nullable (it is a mandatory key)."""
    id_field = EXPECTED_SCHEMA["id"]
    assert id_field.nullable is False


def test_expected_schema_name_nullable():
    """The 'name' field must be nullable."""
    name_field = EXPECTED_SCHEMA["name"]
    assert name_field.nullable is True


def test_expected_schema_status_nullable():
    """The 'status' field must be nullable."""
    status_field = EXPECTED_SCHEMA["status"]
    assert status_field.nullable is True


# --- Data quality checks using EXPECTED_SCHEMA ---


def test_null_name_allowed_by_schema(spark):
    """Rows with a null 'name' value are accepted by EXPECTED_SCHEMA (nullable=True)."""
    df = spark.createDataFrame([(1, None, "Active")], schema=EXPECTED_SCHEMA)
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["name"] is None


def test_null_status_allowed_by_schema(spark):
    """Rows with a null 'status' value are accepted by EXPECTED_SCHEMA (nullable=True)."""
    df = spark.createDataFrame([(2, "Bob", None)], schema=EXPECTED_SCHEMA)
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["status"] is None


def test_full_row_with_expected_schema(spark):
    """A fully populated row is stored and retrieved correctly with EXPECTED_SCHEMA."""
    df = spark.createDataFrame([(42, "Alice", "Active")], schema=EXPECTED_SCHEMA)
    row = df.collect()[0]
    assert row["id"] == 42
    assert row["name"] == "Alice"
    assert row["status"] == "Active"


def test_multiple_rows_with_expected_schema(spark):
    """Multiple rows with varying nullability are stored correctly with EXPECTED_SCHEMA."""
    data = [
        (1, "Alice", "Active"),
        (2, None, "Inactive"),
        (3, "Charlie", None),
    ]
    df = spark.createDataFrame(data, schema=EXPECTED_SCHEMA)
    assert df.count() == 3


def test_null_count_in_name_column(spark):
    """Verify null counting works for the nullable 'name' column."""
    data = [
        (1, "Alice", "Active"),
        (2, None, "Inactive"),
        (3, None, "Active"),
    ]
    df = spark.createDataFrame(data, schema=EXPECTED_SCHEMA)
    null_count = df.filter(col("name").isNull()).count()
    assert null_count == 2


def test_status_column_distinct_values(spark):
    """Distinct status values can be retrieved from a DataFrame using EXPECTED_SCHEMA."""
    data = [
        (1, "Alice", "Active"),
        (2, "Bob", "Inactive"),
        (3, "Charlie", "Active"),
    ]
    df = spark.createDataFrame(data, schema=EXPECTED_SCHEMA)
    statuses = {row["status"] for row in df.select("status").collect()}
    assert statuses == {"Active", "Inactive"}
