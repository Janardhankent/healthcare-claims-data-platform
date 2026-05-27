import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    trim,
    to_date,
    current_timestamp,
    lit,
    when
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
ERROR_DIR = os.path.join(BASE_DIR, "data", "error")


def create_spark_session():
    """
    Create Spark session for local PySpark processing.
    """

    spark = (
        SparkSession.builder
        .appName("HealthcareClaimsSilverTransformations")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze_table(spark, table_name):
    """
    Read Bronze CSV file.
    """

    file_path = os.path.join(BRONZE_DIR, table_name, f"{table_name}_bronze.csv")

    print("=" * 80)
    print(f"Reading Bronze table: {table_name}")
    print(f"Input path: {file_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )

    print(f"Bronze count for {table_name}: {df.count()}")
    return df


def write_local_csv(df, output_folder, output_file_name):
    """
    Write Spark DataFrame to local CSV using Pandas.
    This avoids local Windows Hadoop write issues.
    """

    os.makedirs(output_folder, exist_ok=True)

    output_file = os.path.join(output_folder, output_file_name)

    pandas_df = df.toPandas()
    pandas_df.to_csv(output_file, index=False)

    print(f"Written file: {output_file}")
    print(f"Record count: {len(pandas_df)}")


def clean_patients(patients_df):
    """
    Clean patients data.
    """

    patients_clean_df = (
        patients_df
        .dropDuplicates(["patient_id"])
        .withColumn("patient_id", trim(col("patient_id")))
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("gender", upper(trim(col("gender"))))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("dob", to_date(col("dob")))
        .withColumn("created_date", to_date(col("created_date")))
        .withColumn("silver_processed_timestamp", current_timestamp())
    )

    return patients_clean_df


def clean_providers(providers_df):
    """
    Clean providers data.
    """

    providers_clean_df = (
        providers_df
        .dropDuplicates(["provider_id"])
        .withColumn("provider_id", trim(col("provider_id")))
        .withColumn("provider_name", trim(col("provider_name")))
        .withColumn("specialty", trim(col("specialty")))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("npi", trim(col("npi")))
        .withColumn("silver_processed_timestamp", current_timestamp())
    )

    return providers_clean_df


def clean_payments(payments_df):
    """
    Clean payments data.
    """

    payments_clean_df = (
        payments_df
        .dropDuplicates(["payment_id"])
        .withColumn("payment_id", trim(col("payment_id")))
        .withColumn("claim_id", trim(col("claim_id")))
        .withColumn("payment_date", to_date(col("payment_date")))
        .withColumn("paid_amount", col("paid_amount").cast("double"))
        .withColumn("payment_method", upper(trim(col("payment_method"))))
        .withColumn("silver_processed_timestamp", current_timestamp())
    )

    return payments_clean_df


def clean_claim_status_history(status_history_df):
    """
    Clean claim status history data.
    """

    status_history_clean_df = (
        status_history_df
        .withColumn("claim_id", trim(col("claim_id")))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("reason", trim(col("reason")))
        .withColumn("silver_processed_timestamp", current_timestamp())
    )

    return status_history_clean_df


def transform_claims(claims_df):
    """
    Clean and validate claims data.
    Valid records go to Silver.
    Invalid records go to Error folder.
    """

    valid_status_values = ["APPROVED", "DENIED", "PENDING", "SUBMITTED"]

    claims_clean_df = (
        claims_df
        .withColumn("claim_id", trim(col("claim_id")))
        .withColumn("patient_id", trim(col("patient_id")))
        .withColumn("provider_id", trim(col("provider_id")))
        .withColumn("diagnosis_code", upper(trim(col("diagnosis_code"))))
        .withColumn("procedure_code", trim(col("procedure_code")))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("claim_date", to_date(col("claim_date")))
        .withColumn("claim_amount", col("claim_amount").cast("double"))
        .withColumn("silver_processed_timestamp", current_timestamp())
    )

    claims_clean_df = claims_clean_df.dropDuplicates(["claim_id"])

    claims_valid_df = claims_clean_df.filter(
        (col("claim_id").isNotNull()) &
        (col("patient_id").isNotNull()) &
        (col("provider_id").isNotNull()) &
        (col("claim_date").isNotNull()) &
        (col("claim_amount") > 0) &
        (col("status").isin(valid_status_values))
    )

    claims_invalid_df = (
        claims_clean_df
        .withColumn(
            "error_reason",
            when(col("claim_id").isNull(), lit("Missing claim_id"))
            .when(col("patient_id").isNull(), lit("Missing patient_id"))
            .when(col("provider_id").isNull(), lit("Missing provider_id"))
            .when(col("claim_date").isNull(), lit("Invalid or missing claim_date"))
            .when(col("claim_amount").isNull(), lit("Missing claim_amount"))
            .when(col("claim_amount") <= 0, lit("Claim amount must be greater than 0"))
            .when(~col("status").isin(valid_status_values), lit("Invalid claim status"))
            .otherwise(lit("Unknown error"))
        )
        .filter(
            (col("claim_id").isNull()) |
            (col("patient_id").isNull()) |
            (col("provider_id").isNull()) |
            (col("claim_date").isNull()) |
            (col("claim_amount").isNull()) |
            (col("claim_amount") <= 0) |
            (~col("status").isin(valid_status_values))
        )
    )

    return claims_valid_df, claims_invalid_df


def main():
    spark = create_spark_session()

    patients_df = read_bronze_table(spark, "patients")
    providers_df = read_bronze_table(spark, "providers")
    claims_df = read_bronze_table(spark, "claims")
    payments_df = read_bronze_table(spark, "payments")
    status_history_df = read_bronze_table(spark, "claim_status_history")

    print("=" * 80)
    print("Starting Silver transformations...")

    patients_clean_df = clean_patients(patients_df)
    providers_clean_df = clean_providers(providers_df)
    payments_clean_df = clean_payments(payments_df)
    status_history_clean_df = clean_claim_status_history(status_history_df)
    claims_valid_df, claims_invalid_df = transform_claims(claims_df)

    print("=" * 80)
    print("Silver record counts:")
    print(f"Patients clean count: {patients_clean_df.count()}")
    print(f"Providers clean count: {providers_clean_df.count()}")
    print(f"Payments clean count: {payments_clean_df.count()}")
    print(f"Claim status history clean count: {status_history_clean_df.count()}")
    print(f"Claims valid count: {claims_valid_df.count()}")
    print(f"Claims invalid count: {claims_invalid_df.count()}")

    print("=" * 80)
    print("Writing Silver outputs...")

    write_local_csv(
        patients_clean_df,
        os.path.join(SILVER_DIR, "patients_clean"),
        "patients_clean.csv"
    )

    write_local_csv(
        providers_clean_df,
        os.path.join(SILVER_DIR, "providers_clean"),
        "providers_clean.csv"
    )

    write_local_csv(
        payments_clean_df,
        os.path.join(SILVER_DIR, "payments_clean"),
        "payments_clean.csv"
    )

    write_local_csv(
        status_history_clean_df,
        os.path.join(SILVER_DIR, "claim_status_history_clean"),
        "claim_status_history_clean.csv"
    )

    write_local_csv(
        claims_valid_df,
        os.path.join(SILVER_DIR, "claims_clean"),
        "claims_clean.csv"
    )

    write_local_csv(
        claims_invalid_df,
        os.path.join(ERROR_DIR, "claims_invalid"),
        "claims_invalid.csv"
    )

    spark.stop()

    print("=" * 80)
    print("Silver transformation completed successfully.")


if __name__ == "__main__":
    main()