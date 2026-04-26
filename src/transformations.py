from pyspark.sql.functions import col, lower


def clean_text_data(df, column_name):
    return df.withColumn(column_name, lower(col(column_name)))
