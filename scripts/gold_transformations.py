import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    year,
    month,
    count,
    sum,
    avg,
    round,
    when,
    datediff
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")


def create_spark_session():
    """
    Create Spark session for local PySpark processing.
    """

    spark = (
        SparkSession.builder
        .appName("HealthcareClaimsGoldTransformations")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_silver_csv(spark, folder_name, file_name):
    """
    Read Silver CSV file.
    """

    file_path = os.path.join(SILVER_DIR, folder_name, file_name)

    print("=" * 80)
    print(f"Reading Silver file: {file_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )

    print(f"Record count: {df.count()}")
    return df


def write_local_csv(df, output_folder_name, output_file_name):
    """
    Write Gold DataFrame locally using Pandas.
    This avoids local Windows Spark/Hadoop write issues.
    """

    output_folder = os.path.join(GOLD_DIR, output_folder_name)
    os.makedirs(output_folder, exist_ok=True)

    output_file = os.path.join(output_folder, output_file_name)

    pandas_df = df.toPandas()
    pandas_df.to_csv(output_file, index=False)

    print(f"Written Gold file: {output_file}")
    print(f"Record count: {len(pandas_df)}")


def build_dim_patient(patients_df):
    """
    Build patient dimension table.
    """

    dim_patient_df = (
        patients_df
        .select(
            "patient_id",
            "first_name",
            "last_name",
            "dob",
            "gender",
            "state"
        )
        .dropDuplicates(["patient_id"])
        .withColumn("gold_processed_timestamp", current_timestamp())
    )

    return dim_patient_df


def build_dim_provider(providers_df):
    """
    Build provider dimension table.
    """

    dim_provider_df = (
        providers_df
        .select(
            "provider_id",
            "provider_name",
            "specialty",
            "state",
            "npi"
        )
        .dropDuplicates(["provider_id"])
        .withColumn("gold_processed_timestamp", current_timestamp())
    )

    return dim_provider_df


def build_fact_claims(claims_df, patients_df, providers_df, payments_df):
    """
    Build claim fact table by joining claims with patient, provider, and payment data.
    """

    payments_agg_df = (
        payments_df
        .groupBy("claim_id")
        .agg(
            sum("paid_amount").alias("total_paid_amount"),
            count("payment_id").alias("payment_count")
        )
    )

    fact_claims_df = (
        claims_df.alias("c")
        .join(
            patients_df.select(
                col("patient_id").alias("p_patient_id"),
                col("state").alias("patient_state"),
                col("gender").alias("patient_gender")
            ).alias("p"),
            col("c.patient_id") == col("p.p_patient_id"),
            "left"
        )
        .join(
            providers_df.select(
                col("provider_id").alias("pr_provider_id"),
                col("provider_name"),
                col("specialty"),
                col("state").alias("provider_state")
            ).alias("pr"),
            col("c.provider_id") == col("pr.pr_provider_id"),
            "left"
        )
        .join(
            payments_agg_df.alias("pay"),
            col("c.claim_id") == col("pay.claim_id"),
            "left"
        )
        .select(
            col("c.claim_id"),
            col("c.patient_id"),
            col("c.provider_id"),
            col("c.claim_date"),
            col("c.diagnosis_code"),
            col("c.procedure_code"),
            col("c.claim_amount"),
            col("c.status"),
            col("patient_state"),
            col("patient_gender"),
            col("provider_name"),
            col("specialty"),
            col("provider_state"),
            when(col("total_paid_amount").isNull(), 0).otherwise(col("total_paid_amount")).alias("total_paid_amount"),
            when(col("payment_count").isNull(), 0).otherwise(col("payment_count")).alias("payment_count")
        )
        .withColumn(
            "claim_balance_amount",
            round(col("claim_amount") - col("total_paid_amount"), 2)
        )
        .withColumn(
            "is_denied",
            when(col("status") == "DENIED", 1).otherwise(0)
        )
        .withColumn(
            "is_approved",
            when(col("status") == "APPROVED", 1).otherwise(0)
        )
        .withColumn("gold_processed_timestamp", current_timestamp())
    )

    return fact_claims_df


def build_claim_summary_monthly(fact_claims_df):
    """
    Build monthly claims summary table.
    """

    monthly_summary_df = (
        fact_claims_df
        .withColumn("claim_year", year(col("claim_date")))
        .withColumn("claim_month", month(col("claim_date")))
        .groupBy("claim_year", "claim_month")
        .agg(
            count("claim_id").alias("total_claims"),
            sum("claim_amount").alias("total_claim_amount"),
            sum("total_paid_amount").alias("total_paid_amount"),
            avg("claim_amount").alias("avg_claim_amount"),
            sum("is_denied").alias("denied_claims"),
            sum("is_approved").alias("approved_claims")
        )
        .withColumn(
            "denial_rate_percent",
            round((col("denied_claims") / col("total_claims")) * 100, 2)
        )
        .withColumn(
            "approval_rate_percent",
            round((col("approved_claims") / col("total_claims")) * 100, 2)
        )
        .withColumn("gold_processed_timestamp", current_timestamp())
        .orderBy("claim_year", "claim_month")
    )

    return monthly_summary_df


def build_provider_performance_summary(fact_claims_df):
    """
    Build provider performance summary table.
    """

    provider_summary_df = (
        fact_claims_df
        .groupBy(
            "provider_id",
            "provider_name",
            "specialty",
            "provider_state"
        )
        .agg(
            count("claim_id").alias("total_claims"),
            sum("claim_amount").alias("total_claim_amount"),
            sum("total_paid_amount").alias("total_paid_amount"),
            avg("claim_amount").alias("avg_claim_amount"),
            sum("is_denied").alias("denied_claims"),
            sum("is_approved").alias("approved_claims")
        )
        .withColumn(
            "denial_rate_percent",
            round((col("denied_claims") / col("total_claims")) * 100, 2)
        )
        .withColumn(
            "approval_rate_percent",
            round((col("approved_claims") / col("total_claims")) * 100, 2)
        )
        .withColumn("gold_processed_timestamp", current_timestamp())
        .orderBy(col("total_claim_amount").desc())
    )

    return provider_summary_df


def build_denial_summary(fact_claims_df):
    """
    Build denial summary by diagnosis code and provider specialty.
    """

    denial_summary_df = (
        fact_claims_df
        .filter(col("status") == "DENIED")
        .groupBy(
            "diagnosis_code",
            "specialty",
            "provider_state"
        )
        .agg(
            count("claim_id").alias("denied_claims"),
            sum("claim_amount").alias("denied_claim_amount"),
            avg("claim_amount").alias("avg_denied_claim_amount")
        )
        .withColumn("gold_processed_timestamp", current_timestamp())
        .orderBy(col("denied_claims").desc())
    )

    return denial_summary_df


def build_payment_summary(fact_claims_df):
    """
    Build payment summary table.
    """

    payment_summary_df = (
        fact_claims_df
        .groupBy("status")
        .agg(
            count("claim_id").alias("total_claims"),
            sum("claim_amount").alias("total_claim_amount"),
            sum("total_paid_amount").alias("total_paid_amount"),
            avg("total_paid_amount").alias("avg_paid_amount"),
            sum("claim_balance_amount").alias("total_balance_amount")
        )
        .withColumn(
            "payment_rate_percent",
            round((col("total_paid_amount") / col("total_claim_amount")) * 100, 2)
        )
        .withColumn("gold_processed_timestamp", current_timestamp())
        .orderBy("status")
    )

    return payment_summary_df


def main():
    spark = create_spark_session()

    print("=" * 80)
    print("Starting Gold layer transformations...")

    patients_df = read_silver_csv(
        spark,
        "patients_clean",
        "patients_clean.csv"
    )

    providers_df = read_silver_csv(
        spark,
        "providers_clean",
        "providers_clean.csv"
    )

    claims_df = read_silver_csv(
        spark,
        "claims_clean",
        "claims_clean.csv"
    )

    payments_df = read_silver_csv(
        spark,
        "payments_clean",
        "payments_clean.csv"
    )

    print("=" * 80)
    print("Building Gold dimension and fact tables...")

    dim_patient_df = build_dim_patient(patients_df)
    dim_provider_df = build_dim_provider(providers_df)
    fact_claims_df = build_fact_claims(
        claims_df,
        patients_df,
        providers_df,
        payments_df
    )

    print("=" * 80)
    print("Building Gold summary tables...")

    monthly_summary_df = build_claim_summary_monthly(fact_claims_df)
    provider_summary_df = build_provider_performance_summary(fact_claims_df)
    denial_summary_df = build_denial_summary(fact_claims_df)
    payment_summary_df = build_payment_summary(fact_claims_df)

    print("=" * 80)
    print("Gold record counts:")
    print(f"dim_patient count: {dim_patient_df.count()}")
    print(f"dim_provider count: {dim_provider_df.count()}")
    print(f"fact_claims count: {fact_claims_df.count()}")
    print(f"claim_summary_monthly count: {monthly_summary_df.count()}")
    print(f"provider_performance_summary count: {provider_summary_df.count()}")
    print(f"denial_summary count: {denial_summary_df.count()}")
    print(f"payment_summary count: {payment_summary_df.count()}")

    print("=" * 80)
    print("Writing Gold outputs...")

    write_local_csv(
        dim_patient_df,
        "dim_patient",
        "dim_patient.csv"
    )

    write_local_csv(
        dim_provider_df,
        "dim_provider",
        "dim_provider.csv"
    )

    write_local_csv(
        fact_claims_df,
        "fact_claims",
        "fact_claims.csv"
    )

    write_local_csv(
        monthly_summary_df,
        "claim_summary_monthly",
        "claim_summary_monthly.csv"
    )

    write_local_csv(
        provider_summary_df,
        "provider_performance_summary",
        "provider_performance_summary.csv"
    )

    write_local_csv(
        denial_summary_df,
        "denial_summary",
        "denial_summary.csv"
    )

    write_local_csv(
        payment_summary_df,
        "payment_summary",
        "payment_summary.csv"
    )

    spark.stop()

    print("=" * 80)
    print("Gold transformation completed successfully.")


if __name__ == "__main__":
    main()