import os
import sqlite3
import pandas as pd


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
DB_DIR = os.path.join(BASE_DIR, "data", "database")

os.makedirs(DB_DIR, exist_ok=True)

DB_PATH = os.path.join(DB_DIR, "healthcare_claims.db")


def load_csv_to_sqlite(table_name, file_path, connection):
    print("=" * 80)
    print(f"Loading table: {table_name}")
    print(f"File path: {file_path}")

    df = pd.read_csv(file_path)

    df.to_sql(
        table_name,
        connection,
        if_exists="replace",
        index=False
    )

    print(f"Loaded {len(df)} records into table: {table_name}")


def main():
    connection = sqlite3.connect(DB_PATH)

    gold_tables = {
        "dim_patient": os.path.join(GOLD_DIR, "dim_patient", "dim_patient.csv"),
        "dim_provider": os.path.join(GOLD_DIR, "dim_provider", "dim_provider.csv"),
        "fact_claims": os.path.join(GOLD_DIR, "fact_claims", "fact_claims.csv"),
        "claim_summary_monthly": os.path.join(GOLD_DIR, "claim_summary_monthly", "claim_summary_monthly.csv"),
        "provider_performance_summary": os.path.join(GOLD_DIR, "provider_performance_summary", "provider_performance_summary.csv"),
        "denial_summary": os.path.join(GOLD_DIR, "denial_summary", "denial_summary.csv"),
        "payment_summary": os.path.join(GOLD_DIR, "payment_summary", "payment_summary.csv")
    }

    for table_name, file_path in gold_tables.items():
        load_csv_to_sqlite(table_name, file_path, connection)

    connection.close()

    print("=" * 80)
    print(f"SQLite database created successfully: {DB_PATH}")


if __name__ == "__main__":
    main()