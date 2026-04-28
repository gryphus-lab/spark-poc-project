from pyspark.sql.functions import current_timestamp
from src.utils import get_spark_session
from src.transformations import upsert_to_silver, clean_text_data, EXPECTED_SCHEMA
import logging

logger = logging.getLogger(__name__)


def main(input_path="s3a://warehouse/input/sample.csv"):
    spark = get_spark_session("Silver-Upsert-Job")

    try:
        # Create namespace if not exists
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

        # 1. Read Raw (Bronze) Data from MinIO
        # In a real lake, you'd read from s3a://warehouse/bronze/
        raw_df = spark.read.schema(EXPECTED_SCHEMA).csv(input_path)

        # Validate raw_df
        if raw_df.isEmpty():
            raise ValueError(f"Input file at {input_path} is empty or not found")
        for col in EXPECTED_SCHEMA.fieldNames():
            if col not in raw_df.columns:
                raise ValueError(f"Required column '{col}' missing in input data")

        # 2. Clean and add Metadata
        cleaned_df = clean_text_data(raw_df, "name")
        silver_ready_df = cleaned_df.withColumn("updated_at", current_timestamp())

        # 3. Upsert into Silver Table (Iceberg Format)
        # The 'local' catalog was defined in your SparkSession config earlier
        upsert_to_silver(
            spark, silver_ready_df, "local.db.silver_users", partition_spec="status"
        )

        print("Silver layer upsert complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
