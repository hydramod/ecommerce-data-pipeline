from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "data",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "rt_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,  # CHANGED: Set to None for manual triggers of long-running streams
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "streaming"], # ADDED: Tags for better organization
) as dag:
    orders_bronze = SparkSubmitOperator(
        task_id="orders_bronze_stream",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_orders.py",
        name="orders-bronze",
        application_args=[],
    )
    payments_bronze = SparkSubmitOperator(
        task_id="payments_bronze_stream",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_payments.py",
        name="payments-bronze",
    )
    silver_orders = SparkSubmitOperator(
        task_id="silver_orders_stream",
        conn_id="spark_default",
        application="/opt/spark-jobs/silver_orders.py",
        name="silver-orders",
    )
    silver_payments = SparkSubmitOperator(
        task_id="silver_payments_stream",
        conn_id="spark_default",
        application="/opt/spark-jobs/silver_payments.py",
        name="silver-payments",
    )
    silver_enrich = SparkSubmitOperator(
        task_id="silver_enrich_stream",
        conn_id="spark_default",
        application="/opt/spark-jobs/silver_enrich.py",
        name="silver-enrich",
    )

    [orders_bronze, payments_bronze] >> [silver_orders, silver_payments] >> silver_enrich
