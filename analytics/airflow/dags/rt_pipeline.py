from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ---------------------------------------------------------------------------
# Common config
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Where your Spark job scripts live inside the containers
JOBS_DIR = "/opt/jobs"

# Spark connection id (configure in Airflow UI → Admin → Connections → spark_default)
SPARK_CONN_ID = "spark_default"

# Extra application args (optional). Keep empty unless your scripts accept args.
BRONZE_APP_ARGS = []
SILVER_APP_ARGS = []

# ---------------------------------------------------------------------------
# DAG 1: Bronze streams (always-on)
#   - Reads from Kafka, writes raw events to Delta (MinIO) with checkpoints
#   - Start once and keep running
# ---------------------------------------------------------------------------

with DAG(
    dag_id="bronze_streams",
    description="Continuously ingest Kafka events to Delta bronze (orders, payments)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 8, 1),
    schedule="@once",          # start once, keep running
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "streaming", "bronze"],
) as bronze_dag:

    orders_bronze_stream = SparkSubmitOperator(
        task_id="orders_bronze_stream",
        conn_id=SPARK_CONN_ID,
        application=f"{JOBS_DIR}/bronze_orders.py",
        # If your script needs args, add them:
        application_args=BRONZE_APP_ARGS,
        verbose=True,
    )

    payments_bronze_stream = SparkSubmitOperator(
        task_id="payments_bronze_stream",
        conn_id=SPARK_CONN_ID,
        application=f"{JOBS_DIR}/bronze_payments.py",
        application_args=BRONZE_APP_ARGS,
        verbose=True,
    )

    # No dependency between the two bronze streams; they run independently
    # If you want both started before marking DAG "running", put them in a dummy join.
    # For streaming, it's fine to just launch both.

# ---------------------------------------------------------------------------
# DAG 2: Silver micro-batch (periodic)
#   - Reads Delta bronze tables and produces cleaned/joined silver tables
#   - Runs every 5 minutes by default
# ---------------------------------------------------------------------------

with DAG(
    dag_id="silver_jobs",
    description="Periodic silver transforms and joins (orders, payments → enriched)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 8, 1),
    schedule="*/5 * * * *",    # change to "*/2 * * * *" for a snappier demo
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "batch", "silver"],
) as silver_dag:

    # If you already have these scripts, keep the paths.
    # Otherwise, point them to your actual silver job scripts.
    silver_orders = SparkSubmitOperator(
        task_id="silver_orders",
        conn_id=SPARK_CONN_ID,
        application=f"{JOBS_DIR}/silver_orders.py",
        application_args=SILVER_APP_ARGS,
        verbose=True,
    )

    silver_payments = SparkSubmitOperator(
        task_id="silver_payments",
        conn_id=SPARK_CONN_ID,
        application=f"{JOBS_DIR}/silver_payments.py",
        application_args=SILVER_APP_ARGS,
        verbose=True,
    )

    silver_enrich = SparkSubmitOperator(
        task_id="silver_enrich",
        conn_id=SPARK_CONN_ID,
        application=f"{JOBS_DIR}/silver_enrich.py",
        application_args=SILVER_APP_ARGS,
        verbose=True,
    )

    # Both silver tables first → then enrichment/join
    chain([silver_orders, silver_payments], silver_enrich)
