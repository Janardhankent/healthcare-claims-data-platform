import os
import pandas as pd


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

REQUIRED_FILES = {
    "raw_patients": os.path.join(BASE_DIR, "data", "raw", "patients.csv"),
    "raw_providers": os.path.join(BASE_DIR, "data", "raw", "providers.csv"),
    "raw_claims": os.path.join(BASE_DIR, "data", "raw", "claims.csv"),
    "bronze_patients": os.path.join(BASE_DIR, "data", "bronze", "patients", "patients_bronze.csv"),
    "bronze_claims": os.path.join(BASE_DIR, "data", "bronze", "claims", "claims_bronze.csv"),
    "silver_claims": os.path.join(BASE_DIR, "data", "silver", "claims_clean", "claims_clean.csv"),
    "error_claims": os.path.join(BASE_DIR, "data", "error", "claims_invalid", "claims_invalid.csv"),
    "gold_fact_claims": os.path.join(BASE_DIR, "data", "gold", "fact_claims", "fact_claims.csv"),
    "gold_provider_summary": os.path.join(
        BASE_DIR,
        "data",
        "gold",
        "provider_performance_summary",
        "provider_performance_summary.csv"
    ),
}


def validate_file_exists(file_name, file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing required file: {file_name} at {file_path}")

    print(f"File exists: {file_name}")


def validate_file_has_records(file_name, file_path):
    df = pd.read_csv(file_path)

    if len(df) == 0:
        raise ValueError(f"File has zero records: {file_name}")

    print(f"{file_name} record count: {len(df)}")


def main():
    print("=" * 80)
    print("Starting pipeline output validation...")

    for file_name, file_path in REQUIRED_FILES.items():
        validate_file_exists(file_name, file_path)
        validate_file_has_records(file_name, file_path)

    print("=" * 80)
    print("Pipeline validation completed successfully.")


if __name__ == "__main__":
    main()