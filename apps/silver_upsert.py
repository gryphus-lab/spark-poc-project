from pyspark.sql.functions import current_timestamp
from src.utils import get_spark_session
from src.transformations import upsert_to_silver, clean_text_data, EXPECTED_SCHEMA


def main():
    spark = get_spark_session("Silver-Upsert-Job")

    try:
        # 1. Read Raw (Bronze) Data from MinIO
        # In a real lake, you'd read from s3a://warehouse/bronze/
        raw_df = spark.read.schema(EXPECTED_SCHEMA).csv("s3a://warehouse/input/sample.csv")

        # 2. Clean and add Metadata
        cleaned_df = clean_text_data(raw_df, "name")
        silver_ready_df = cleaned_df.withColumn("updated_at", current_timestamp())

        # 3. Upsert into Silver Table (Iceberg Format)
        # The 'local' catalog was defined in your SparkSession config earlier
        upsert_to_silver(spark, silver_ready_df, "local.db.silver_users", partition_spec="status")

        print("Silver layer upsert complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
