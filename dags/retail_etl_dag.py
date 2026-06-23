from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "you",
    "retries": 1,
}

with DAG(
    dag_id="retail_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 6, 17),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    raw_to_bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command=(
            "python /opt/airflow/spark_jobs/"
            "01_ingest_raw_to_bronze.py"
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "python /opt/airflow/spark_jobs/"
            "02_clean_and_enrich_silver.py"
        ),
    )

    data_quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command=(
            "python /opt/airflow/spark_jobs/"
            "03_validate_data_quality.py"
        ),
    )

    build_star_schema = BashOperator(
        task_id="build_star_schema",
        bash_command=(
            "python /opt/airflow/spark_jobs/"
            "04_build_star_schema.py"
        ),
    )

    raw_to_bronze >> bronze_to_silver
    bronze_to_silver >> data_quality_checks
    data_quality_checks >> build_star_schema