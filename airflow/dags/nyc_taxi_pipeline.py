"""Manually triggered, parameterized three-month ETL + dbt workflow.

Airflow UI defaults to July-September 2026 and 20,000 source rows per month.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator

PYTHON = "/home/airflow/project-venv/bin/python"
DBT = "/home/airflow/project-venv/bin/dbt"

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="Parameterized NYC Taxi Data Engineering Pipeline",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    params={
        "year": Param(2026, type="integer", minimum=2020, maximum=2030),
        "month_1": Param(7, type="integer", minimum=1, maximum=12),
        "month_2": Param(8, type="integer", minimum=1, maximum=12),
        "month_3": Param(9, type="integer", minimum=1, maximum=12),
        "limit": Param(20000, type="integer", minimum=1, maximum=100000),
    },
    tags=["data-engineering", "nyc-taxi", "parameterized"],
) as dag:

    previous_task = None

    for slot in (1, 2, 3):
        ingest_task = BashOperator(
            task_id=f"ingest_month_{slot}",
            bash_command=(
                "cd /opt/project && "
                f"PYTHONPATH=/opt/project {PYTHON} "
                "-m src.ingest "
                "--year {{ params.year }} "
                f"--month {{{{ params.month_{slot} }}}} "
                "--limit {{ params.limit }}"
            ),
        )

        if previous_task is not None:
            previous_task >> ingest_task

        previous_task = ingest_task

    # Quoted YAML mapping is passed to both dbt commands by BashOperator.
    dbt_vars = (
        '"{year: {{ params.year }}, '
        'months: [{{ params.month_1 }}, {{ params.month_2 }}, {{ params.month_3 }}]}"'
    )

    build_models = BashOperator(
        task_id="build_dbt_models",
        bash_command=(
            f"{DBT} run "
            "--project-dir /opt/project/dbt "
            "--profiles-dir /opt/project/airflow_profiles "
            f"--vars {dbt_vars}"
        ),
    )

    run_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"{DBT} test "
            "--project-dir /opt/project/dbt "
            "--profiles-dir /opt/project/airflow_profiles "
            f"--vars {dbt_vars}"
        ),
    )

    previous_task >> build_models >> run_tests
