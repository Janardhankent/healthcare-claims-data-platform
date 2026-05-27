import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")


def create_spark_session():
    """
    Create Spark session for local PySpark processing.
    """

    spark = (
        SparkSession.builder
        .appName("HealthcareClaimsBronzeIngestion")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def read_csv_file(spark, file_path):
    """
    Read CSV file using PySpark.
    """

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )

    return df


def add_bronze_metadata(df, source_name):
    """
    Add bronze layer metadata columns.
    """

    df_with_metadata = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit("healthcare_claims_csv"))
        .withColumn("source_file_name", input_file_name())
        .withColumn("bronze_table_name", lit(source_name))
    )

    return df_with_metadata


def write_to_bronze(df, source_name):
    """
    Write dataframe to bronze folder using Pandas for local Windows testing.
    This avoids local Spark/Hadoop Windows write issues.
    Later this will be changed to Parquet/Delta in Databricks.
    """

    output_path = os.path.join(BRONZE_DIR, source_name)
    os.makedirs(output_path, exist_ok=True)

    output_file = os.path.join(output_path, f"{source_name}_bronze.csv")

    pandas_df = df.toPandas()
    pandas_df.to_csv(output_file, index=False)

    print(f"Successfully written bronze table: {source_name}")
    print(f"Output file: {output_file}")


def process_source_file(spark, source_name, file_name):
    """
    Process one source CSV file from raw to bronze.
    """

    file_path = os.path.join(RAW_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print("=" * 80)
    print(f"Processing source: {source_name}")
    print(f"Input file: {file_path}")

    raw_df = read_csv_file(spark, file_path)

    print(f"Raw record count for {source_name}: {raw_df.count()}")

    bronze_df = add_bronze_metadata(raw_df, source_name)

    print(f"Bronze schema for {source_name}:")
    bronze_df.printSchema()

    write_to_bronze(bronze_df, source_name)

    print(f"Completed processing: {source_name}")


def main():
    spark = create_spark_session()

    source_files = {
        "patients": "patients.csv",
        "providers": "providers.csv",
        "claims": "claims.csv",
        "payments": "payments.csv",
        "claim_status_history": "claim_status_history.csv"
    }

    for source_name, file_name in source_files.items():
        process_source_file(spark, source_name, file_name)

    spark.stop()

    print("=" * 80)
    print("Bronze ingestion completed successfully.")


if __name__ == "__main__":
    main()