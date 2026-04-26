import sys
import os

# Add src to path so Spark can find our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils import get_spark_session
from src.transformations import clean_text_data


def main():
    spark = get_spark_session()

    # Load data
    df = spark.read.csv("/opt/bitnami/spark/data/input/sample.csv", header=True)

    # Process
    cleaned_df = clean_text_data(df, "name")

    # Save
    cleaned_df.write.mode("overwrite").parquet(
        "/opt/bitnami/spark/data/output/processed_data"
    )

    spark.stop()


if __name__ == "__main__":
    main()
