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


def normalize_sql(sql):
    """
    Normalize a SQL string for stable comparisons by collapsing whitespace and lowercasing.

    Parameters:
        sql (str): The SQL text to normalize.

    Returns:
        normalized_sql (str): The input SQL with all runs of whitespace replaced by a single space and converted to lowercase.
    """
    return " ".join(sql.split()).lower()


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
    # Handle both positional and keyword arguments robustly
    sql_fragments = []
    for call in mock_spark.sql.call_args_list:
        if call.args:
            sql_fragments.append(call.args[0])
        elif "sql" in call.kwargs:
            sql_fragments.append(call.kwargs["sql"])
        elif "sqlText" in call.kwargs:
            sql_fragments.append(call.kwargs["sqlText"])
        else:
            sql_fragments.append("")
    all_sql_executed = normalize_sql(" ".join(sql_fragments))

    # Verify DDL
    assert "create table if not exists local.db.users" in all_sql_executed
    assert "using iceberg" in all_sql_executed
    assert "partitioned by (status)" in all_sql_executed

    # Verify Merge Logic
    assert "merge into local.db.users t" in all_sql_executed
    assert "on t.`id` = s.`id`" in all_sql_executed
    # Check UPDATE SET is present (normalized for whitespace)
    assert "update set" in all_sql_executed
    assert "t.`name` = s.`name`" in all_sql_executed
    assert "t.`status` = s.`status`" in all_sql_executed

    # Verify INSERT is present
    assert "insert" in all_sql_executed
    assert "`id`, `name`, `status`" in all_sql_executed

    # Verify temp view was created
    assert mock_df.createOrReplaceTempView.called

    # Verify cleanup was performed
    assert mock_spark.catalog.dropTempView.called
    # Ensure correct cleanup - the temp view name should match
    temp_view_name = mock_df.createOrReplaceTempView.call_args[0][0]
    assert mock_spark.catalog.dropTempView.call_args[0][0] == temp_view_name
