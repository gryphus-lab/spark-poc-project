import pytest
from src.transformations import clean_text_data

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
