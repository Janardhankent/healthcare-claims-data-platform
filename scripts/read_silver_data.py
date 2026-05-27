import os
import sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
ERROR_DIR = os.path.join(BASE_DIR, "data", "error")


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("ReadSilverData")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_csv_file(spark, file_path, table_name):
    print("=" * 80)
    print(f"Reading table: {table_name}")
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

    silver_tables = {
        "patients_clean": os.path.join(SILVER_DIR, "patients_clean", "patients_clean.csv"),
        "providers_clean": os.path.join(SILVER_DIR, "providers_clean", "providers_clean.csv"),
        "claims_clean": os.path.join(SILVER_DIR, "claims_clean", "claims_clean.csv"),
        "payments_clean": os.path.join(SILVER_DIR, "payments_clean", "payments_clean.csv"),
        "claim_status_history_clean": os.path.join(
            SILVER_DIR,
            "claim_status_history_clean",
            "claim_status_history_clean.csv"
        ),
        "claims_invalid": os.path.join(ERROR_DIR, "claims_invalid", "claims_invalid.csv")
    }

    for table_name, file_path in silver_tables.items():
        read_csv_file(spark, file_path, table_name)

    spark.stop()


if __name__ == "__main__":
    main()