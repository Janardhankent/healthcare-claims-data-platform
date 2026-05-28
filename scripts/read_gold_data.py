import os
import sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("ReadGoldData")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_gold_table(spark, table_name):
    file_path = os.path.join(GOLD_DIR, table_name, f"{table_name}.csv")

    print("=" * 80)
    print(f"Reading Gold table: {table_name}")
    print(f"Path: {file_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )

    print(f"Record count: {df.count()}")
    df.printSchema()
    df.show(5, truncate=False)


def main():
    spark = create_spark_session()

    gold_tables = [
        "dim_patient",
        "dim_provider",
        "fact_claims",
        "claim_summary_monthly",
        "provider_performance_summary",
        "denial_summary",
        "payment_summary"
    ]

    for table_name in gold_tables:
        read_gold_table(spark, table_name)

    spark.stop()


if __name__ == "__main__":
    main()