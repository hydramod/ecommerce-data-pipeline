from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DEFAULT_ARGS = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="trino_bootstrap_views",
    description="Idempotent init: create Trino views after silver tables are available",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 8, 1),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    tags=["trino", "bootstrap", "views"],
) as dag:

    create_enriched_view = BashOperator(
        task_id="create_enriched_view",
        bash_command=r"""
set -euo pipefail

TRINO_SERVER="${TRINO_SERVER:-http://trino:8080}"

# Wait for Trino to be up
until trino --server "$TRINO_SERVER" --execute "SELECT 1" >/dev/null 2>&1; do
  echo "Waiting for Trino..."
  sleep 3
done

# Wait until the two base silver tables are visible
wait_for_table () {
  local tbl="$1"
  for i in $(seq 1 60); do
    if trino --server "$TRINO_SERVER" --execute "SHOW TABLES FROM delta.silver" | grep -q "^${tbl}\b"; then
      echo "Found delta.silver.${tbl}"
      return 0
    fi
    echo "Waiting for delta.silver.${tbl}..."
    sleep 5
  done
  echo "Timed out waiting for delta.silver.${tbl}" >&2
  exit 1
}

# Create schema (idempotent)
trino --server "$TRINO_SERVER" --execute "CREATE SCHEMA IF NOT EXISTS delta.silver;"

# Ensure base tables exist first (produced by your silver jobs)
wait_for_table orders_clean
wait_for_table payments_clean

# Create or replace the enriched view (idempotent)
trino --server "$TRINO_SERVER" --execute "
CREATE OR REPLACE VIEW delta.silver.order_payments_enriched AS
WITH pay AS (
  SELECT order_id, SUM(amount) AS paid_amount
  FROM delta.silver.payments_clean
  GROUP BY order_id
)
SELECT
  o.order_id,
  o.total_amount,
  COALESCE(pay.paid_amount, 0.0) AS paid_amount,
  o.status AS order_status,
  (COALESCE(pay.paid_amount, 0.0) >= o.total_amount) AS fully_paid,
  o.currency,
  o.event_ts,
  o.event_date
FROM delta.silver.orders_clean o
LEFT JOIN pay ON pay.order_id = o.order_id;
"
echo "Enriched view created/refreshed."
""",
    )
