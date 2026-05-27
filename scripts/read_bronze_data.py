import os
import sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("ReadBronzeData")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze_table(spark, table_name):
    table_path = os.path.join(BRONZE_DIR, table_name, f"{table_name}_bronze.csv")

    print("=" * 80)
    print(f"Reading bronze table: {table_name}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(table_path)
    )

    print(f"Record count: {df.count()}")
    df.printSchema()
    df.show(5, truncate=False)


def main():
    spark = create_spark_session()

    bronze_tables = [
        "patients",
        "providers",
        "claims",
        "payments",
        "claim_status_history"
    ]

    for table_name in bronze_tables:
        read_bronze_table(spark, table_name)

    spark.stop()


if __name__ == "__main__":
    main()