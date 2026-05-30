import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

load_dotenv()


def get_env_value(name):
    value = os.getenv(name)

    if value is None or value.strip() == "":
        raise ValueError(f"Missing environment variable: {name}")

    return value.strip()


def get_snowflake_connection():
    user = get_env_value("SNOWFLAKE_USER")
    password = get_env_value("SNOWFLAKE_PASSWORD")
    account = get_env_value("SNOWFLAKE_ACCOUNT")
    warehouse = get_env_value("SNOWFLAKE_WAREHOUSE")
    database = get_env_value("SNOWFLAKE_DATABASE")
    schema = get_env_value("SNOWFLAKE_SCHEMA")
    role = get_env_value("SNOWFLAKE_ROLE")

    print("=" * 80)
    print("Connecting to Snowflake with:")
    print(f"Account   : {account}")
    print(f"User      : {user}")
    print(f"Role      : {role}")
    print(f"Warehouse : {warehouse}")
    print(f"Database  : {database}")
    print(f"Schema    : {schema}")
    print("=" * 80)

    connection = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )

    cursor = connection.cursor()

    try:
        cursor.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        result = cursor.fetchone()

        print("Current Snowflake context:")
        print(f"Role      : {result[0]}")
        print(f"Warehouse : {result[1]}")
        print(f"Database  : {result[2]}")
        print(f"Schema    : {result[3]}")
        print("=" * 80)

    finally:
        cursor.close()

    return connection


def normalize_column_names(df):
    df.columns = [column.upper() for column in df.columns]
    return df


def clean_dataframe(df):
    df = df.where(pd.notnull(df), None)
    return df


def truncate_table(connection, database, schema, table_name):
    full_table_name = f"{database}.{schema}.{table_name}"

    cursor = connection.cursor()

    try:
        cursor.execute(f"TRUNCATE TABLE {full_table_name}")
        print(f"Truncated table: {full_table_name}")
    finally:
        cursor.close()


def load_csv_to_snowflake(connection, database, schema, table_name, csv_path):
    print("=" * 80)
    print(f"Loading table: {table_name}")
    print(f"CSV path: {csv_path}")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    df = normalize_column_names(df)
    df = clean_dataframe(df)

    print(f"CSV row count: {len(df)}")
    print(f"CSV columns: {list(df.columns)}")

    truncate_table(connection, database, schema, table_name)

    success, num_chunks, num_rows, output = write_pandas(
        conn=connection,
        df=df,
        table_name=table_name,
        database=database,
        schema=schema,
        quote_identifiers=False,
    )

    if success:
        print(f"Loaded {num_rows} rows into {database}.{schema}.{table_name}")
    else:
        raise RuntimeError(f"Failed loading {table_name}: {output}")


def validate_table_counts(connection, database, schema):
    cursor = connection.cursor()

    tables = [
        "DIM_PATIENT",
        "DIM_PROVIDER",
        "FACT_CLAIMS",
        "CLAIM_SUMMARY_MONTHLY",
        "PROVIDER_PERFORMANCE_SUMMARY",
        "DENIAL_SUMMARY",
        "PAYMENT_SUMMARY",
    ]

    try:
        print("=" * 80)
        print("Snowflake row count validation:")

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count}")

    finally:
        cursor.close()


def main():
    database = get_env_value("SNOWFLAKE_DATABASE")
    schema = get_env_value("SNOWFLAKE_SCHEMA")

    tables = {
        "DIM_PATIENT": os.path.join(GOLD_DIR, "dim_patient", "dim_patient.csv"),
        "DIM_PROVIDER": os.path.join(GOLD_DIR, "dim_provider", "dim_provider.csv"),
        "FACT_CLAIMS": os.path.join(GOLD_DIR, "fact_claims", "fact_claims.csv"),
        "CLAIM_SUMMARY_MONTHLY": os.path.join(
            GOLD_DIR,
            "claim_summary_monthly",
            "claim_summary_monthly.csv",
        ),
        "PROVIDER_PERFORMANCE_SUMMARY": os.path.join(
            GOLD_DIR,
            "provider_performance_summary",
            "provider_performance_summary.csv",
        ),
        "DENIAL_SUMMARY": os.path.join(
            GOLD_DIR,
            "denial_summary",
            "denial_summary.csv",
        ),
        "PAYMENT_SUMMARY": os.path.join(
            GOLD_DIR,
            "payment_summary",
            "payment_summary.csv",
        ),
    }

    connection = get_snowflake_connection()

    try:
        for table_name, csv_path in tables.items():
            load_csv_to_snowflake(
                connection=connection,
                database=database,
                schema=schema,
                table_name=table_name,
                csv_path=csv_path,
            )

        validate_table_counts(connection, database, schema)

        print("=" * 80)
        print("All Gold tables loaded into Snowflake successfully.")

    finally:
        connection.close()


if __name__ == "__main__":
    main()