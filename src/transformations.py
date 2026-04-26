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
