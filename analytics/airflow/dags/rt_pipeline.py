from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator

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

    payments_bronze_stream = BashOperator(
        task_id="payments_bronze_stream",
        bash_command=f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client {JOBS_DIR}/bronze_payments.py",
        pool="streaming"
    )

    orders_bronze_stream = BashOperator(
        task_id="orders_bronze_stream",
        bash_command=f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client {JOBS_DIR}/bronze_orders.py",
        pool="streaming"
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
    silver_orders = BashOperator(
        task_id="silver_orders",
        bash_command=f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client {JOBS_DIR}/silver_orders.py",
    )
    silver_payments = BashOperator(
        task_id="silver_payments",
        bash_command=f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client {JOBS_DIR}/silver_payments.py",
    )
    silver_enrich = BashOperator(
        task_id="silver_enrich",
        bash_command=f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client {JOBS_DIR}/silver_enrich.py",
    )

    # Both silver tables first → then enrichment/join
    chain([silver_orders, silver_payments], silver_enrich)
