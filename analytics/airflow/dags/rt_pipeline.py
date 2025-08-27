from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

LAKE_PATH = os.getenv("LAKE_PATH", "/lake")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# These jobs run inside the spark-master container using spark-submit
SPARK_SUBMIT = "spark-submit --master spark://spark-master:7077"

DAG_ID = "rt_pipeline"

with DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,  # trigger manually or from CI; streaming jobs are long-running
    catchup=False,
    tags=["realtime", "streaming", "delta"],
) as dag:

    orders_bronze = BashOperator(
        task_id="orders_bronze_stream",
        bash_command=f"docker exec spark-master {SPARK_SUBMIT} /opt/jobs/bronze_orders.py",
    )

    payments_bronze = BashOperator(
        task_id="payments_bronze_stream",
        bash_command=f"docker exec spark-master {SPARK_SUBMIT} /opt/jobs/bronze_payments.py",
    )

    silver_enrich = BashOperator(
        task_id="silver_enrich_stream",
        bash_command=f"docker exec spark-master {SPARK_SUBMIT} /opt/jobs/silver_enrich.py",
    )

    # The silver job depends on both bronze ingests being up first
    [orders_bronze, payments_bronze] >> silver_enrich