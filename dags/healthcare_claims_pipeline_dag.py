from datetime import datetime, timedelta
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = "/opt/airflow"


default_args = {
    "owner": "janardhan",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def run_python_script(script_name):
    script_path = f"{PROJECT_ROOT}/scripts/{script_name}"

    print("=" * 80)
    print(f"Running script: {script_path}")

    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True,
    )

    print("STDOUT:")
    print(result.stdout)

    print("STDERR:")
    print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Script failed: {script_name}")


with DAG(
    dag_id="healthcare_claims_data_platform",
    default_args=default_args,
    description="End-to-end healthcare claims data pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["healthcare", "claims", "data-engineering"],
) as dag:

    generate_raw_data = PythonOperator(
        task_id="generate_raw_data",
        python_callable=run_python_script,
        op_args=["generate_sample_data.py"],
    )

    bronze_ingestion = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_python_script,
        op_args=["bronze_ingestion.py"],
    )

    silver_transformations = PythonOperator(
        task_id="silver_transformations",
        python_callable=run_python_script,
        op_args=["silver_transformations.py"],
    )

    gold_transformations = PythonOperator(
        task_id="gold_transformations",
        python_callable=run_python_script,
        op_args=["gold_transformations.py"],
    )

    load_gold_to_snowflake = PythonOperator(
        task_id="load_gold_to_snowflake",
        python_callable=run_python_script,
        op_args=["load_gold_to_snowflake.py"],
    )

    validate_pipeline_outputs = PythonOperator(
        task_id="validate_pipeline_outputs",
        python_callable=run_python_script,
        op_args=["validate_pipeline_outputs.py"],
    )

    (
        generate_raw_data
        >> bronze_ingestion
        >> silver_transformations
        >> gold_transformations
        >> load_gold_to_snowflake
        >> validate_pipeline_outputs
    )