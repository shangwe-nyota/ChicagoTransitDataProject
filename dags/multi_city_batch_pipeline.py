from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BATCH_RUN_ID = "airflow-{{ ts_nodash }}"


def build_city_batch_task(city: str) -> BashOperator:
    return BashOperator(
        task_id=f"run_{city}_batch_pipeline",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python jobs/pipeline/run_city_batch_pipeline.py --city {city} --run-id {BATCH_RUN_ID}"
        ),
    )


with DAG(
    dag_id="multi_city_batch_pipeline",
    description="Daily city-aware GTFS + OSM batch pipeline for Chicago and Boston.",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["batch", "gtfs", "osm", "snowflake", "spark", "multi-city"],
) as dag:
    chicago_batch = build_city_batch_task("chicago")
    boston_batch = build_city_batch_task("boston")

    load_to_snowflake = BashOperator(
        task_id="load_multi_city_batch_to_snowflake",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python -m jobs.load.load_to_snowflake --run-id {BATCH_RUN_ID}"
        ),
    )

    [chicago_batch, boston_batch] >> load_to_snowflake
