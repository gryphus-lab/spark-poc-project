import pytest
from unittest.mock import MagicMock
from src.transformations import clean_text_data, upsert_to_silver


@pytest.fixture
def mock_spark():
    """Fixture for mocking Spark when testing SQL generation logic."""
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    return spark


def test_clean_text_data_lowercases_via_spark_fixture(spark):
    # Use the 'spark' fixture from your conftest.py
    data = [("JOE",), ("Alice",)]
    df = spark.createDataFrame(data, ["name"])

    result_df = clean_text_data(df, "name")
    results = result_df.collect()

    # Results are Row objects, check values by index or attribute
    assert results[0]["name"] == "joe"
    assert results[1]["name"] == "alice"


def test_upsert_to_silver_creates_correct_sql(mock_spark):
    # Setup mock DataFrame with fields that have a simpleString method
    mock_df = MagicMock()
    mock_df.columns = ["id", "name", "status"]

    f1 = MagicMock()
    f1.name = "id"
    f1.dataType.simpleString.return_value = "int"
    f2 = MagicMock()
    f2.name = "name"
    f2.dataType.simpleString.return_value = "string"
    f3 = MagicMock()
    f3.name = "status"
    f3.dataType.simpleString.return_value = "string"
    mock_df.schema.fields = [f1, f2, f3]

    # Mock DESCRIBE EXTENDED output for partition checking
    mock_spark.sql.return_value.collect.return_value = [
        {"col_name": "# Partition Information"},
        {"col_name": "status"},
        {"col_name": "# Metadata Information"},
    ]

    upsert_to_silver(
        spark=mock_spark,
        df=mock_df,
        table_name="local.db.users",
        partition_spec="status",
        merge_key="id",
    )

    # Flatten all SQL calls to verify fragments (ignores whitespace/newlines)
    all_sql_executed = " ".join(
        [call.args[0] for call in mock_spark.sql.call_args_list]
    )

    # Verify DDL
    assert "CREATE TABLE IF NOT EXISTS local.db.users" in all_sql_executed
    assert "USING iceberg" in all_sql_executed
    assert "PARTITIONED BY (status)" in all_sql_executed

    # Verify Merge Logic
    assert "MERGE INTO local.db.users t" in all_sql_executed
    assert "ON t.`id` = s.`id`" in all_sql_executed
    # Use a specific fragment to avoid whitespace issues
    assert "UPDATE SET t.`name` = s.`name`, t.`status` = s.`status`" in all_sql_executed
    assert (
        "INSERT (`id`, `name`, `status`) VALUES (s.`id`, s.`name`, s.`status`)"
        in all_sql_executed
    )
