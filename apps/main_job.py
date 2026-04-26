import sys
import os
from pyspark.sql.utils import AnalysisException

# Ensure src is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils import get_spark_session
from src.transformations import clean_text_data, EXPECTED_SCHEMA


def main():
    spark = get_spark_session("Spark-Quality-POC")

    input_path = "/opt/spark/project/data/input/sample.csv"
    output_path = "/opt/spark/project/data/output/processed_data"

    try:
        # 1. Load data with Schema Enforcement
        # mode="FAILFAST" stops the job immediately if data doesn't match the schema
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("mode", "FAILFAST")
            .schema(EXPECTED_SCHEMA)
            .load(input_path)
        )

        # 2. Apply transformations
        processed_df = clean_text_data(df, "name")

        # 3. Write results
        processed_df.write.mode("overwrite").parquet(output_path)
        print(f"Successfully processed data to {output_path}")

    except AnalysisException as e:
        print(f"Data Quality Error: Input data does not match schema. {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
