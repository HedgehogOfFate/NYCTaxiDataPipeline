import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

SPARK_JOBS_DIR = "/opt/airflow/spark_jobs"
DQ_MODULE = "dq_checks.run_checks"
SCRIPTS_DIR = "/opt/airflow/scripts"
DATA_DIR = "/opt/airflow/data"

YEAR = "{{ execution_date.year }}"
MONTH = "{{ execution_date.month }}"

default_args = {
    "owner": "de-portfolio-pipeline",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": dt.timedelta(minutes=20),
    "email_on_failure": False,
}

with DAG(
    dag_id="taxi_pipeline_dag",
    description="NYC TLC Yellow Taxi: bronze -> silver -> gold, with DQ checkpoints after each layer",
    default_args=default_args,
    schedule_interval="@monthly",
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["de-portfolio-pipeline", "taxi"],
) as dag:

    download_raw = BashOperator(
        task_id="download_raw",
        bash_command=(f"python {SCRIPTS_DIR}/download_raw.py --year {YEAR} --month {MONTH}"),
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            f"spark-submit --master $SPARK_MASTER_URL "
            f"{SPARK_JOBS_DIR}/bronze_ingest.py --year {YEAR} --month {MONTH}"
        ),
    )

    dq_check_bronze = BashOperator(
        task_id="dq_check_bronze",
        bash_command=(f"python -m {DQ_MODULE} --layer bronze --path {DATA_DIR}/bronze/yellow_tripdata"),
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=(
            f"spark-submit --master $SPARK_MASTER_URL "
            f"{SPARK_JOBS_DIR}/silver_transform.py --year {YEAR} --month {MONTH}"
        ),
    )

    dq_check_silver = BashOperator(
        task_id="dq_check_silver",
        bash_command=(
            f"BRONZE_ROWS=$(python -m dq_checks.count_rows --path {DATA_DIR}/bronze/yellow_tripdata) && "
            f"python -m {DQ_MODULE} --layer silver --path {DATA_DIR}/silver/yellow_tripdata "
            "--bronze-row-count $BRONZE_ROWS"
        ),
    )

    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command=(f"spark-submit --master $SPARK_MASTER_URL " f"{SPARK_JOBS_DIR}/gold_aggregate.py"),
    )

    gold_tables = [
        "gold_daily_zone_trips",
        "gold_hourly_trips",
        "gold_borough_fares",
        "gold_revenue_trends",
        "gold_tip_by_payment",
    ]
    dq_check_gold_tasks = [
        BashOperator(
            task_id=f"dq_check_gold__{table}",
            bash_command=(f"python -m {DQ_MODULE} --layer gold --path {DATA_DIR}/gold/{table} --table-name {table}"),
        )
        for table in gold_tables
    ]

    notify_failure = BashOperator(
        task_id="notify_failure",
        bash_command='echo "[ALERT] taxi_pipeline_dag failed for {{ ds }} -- check task logs above"',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    download_raw >> bronze_ingest >> dq_check_bronze >> silver_transform >> dq_check_silver >> gold_aggregate
    for task in dq_check_gold_tasks:
        gold_aggregate >> task

    [
        download_raw,
        bronze_ingest,
        dq_check_bronze,
        silver_transform,
        dq_check_silver,
        gold_aggregate,
        *dq_check_gold_tasks,
    ] >> notify_failure
