from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="chicago_static_pipeline",
        start_date=datetime(2026, 4, 1),
        schedule="@daily",  # run the piepline everyday at midnight
        catchup=False,
        tags=["cta", "gtfs", "snowflake", "spark"],
) as dag:
    download_gtfs = BashOperator(
        task_id="download_gtfs",
        bash_command="python jobs/ingestion/download_gtfs.py",
    )

    clean_gtfs = BashOperator(
        task_id="clean_gtfs",
        bash_command="python jobs/spark/clean_gtfs.py",
    )

    build_analytics = BashOperator(
        task_id="build_analytics",
        bash_command="python jobs/spark/build_analytics.py",
    )

    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command="python -m jobs.load.load_to_snowflake",
    )

    validate_data = BashOperator(
        task_id="validate_data",
        bash_command="python jobs/validation/validate_data.py",
    )

    download_gtfs >> clean_gtfs >> build_analytics >> load_to_snowflake >> validate_data
