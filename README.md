# Ecommerce Microservices – Analytics Pipeline

Real-time analytics over the e-commerce event stream using **Kafka → Spark (Bronze/Silver) → dbt (Gold) → Trino → Metabase**. The pipeline delivers near-real-time sales metrics with tests, freshness checks, and lineage.

---

## Architecture (Medallion)

```
Kafka Topics
   └─► Spark Structured Streaming  ──►  Delta Lake (Bronze)
           └─► Spark Batch         ──►  Delta Lake (Silver)
                   └─► dbt (Trino) ──►  Delta Lake (Gold)
                               └─► Trino SQL
                                      └─► Metabase (dashboard)
```

* **Bronze** = raw Kafka payloads (append-only)
* **Silver** = cleaned & lightly enriched tables
* **Gold**   = business-ready facts built by dbt

---

## Components

### 1) Data Ingestion (Bronze)

* **Tech:** Apache Spark Structured Streaming
* **Sources:** Kafka topics `orders.events`, `payments.events`
* **Destination:** Delta Lake on MinIO (S3-compatible)
* **Tables:**

  * `bronze_raw.orders_raw`
  * `bronze_raw.payments_raw`

### 2) Data Processing (Silver)

* **Tech:** Apache Spark (batch)
* **Process:** dedupe, schema validation, type casting, basic enrichment
* **Tables / Views:**

  * `silver.orders_clean`
  * `silver.payments_clean`
  * `silver.order_payments_enriched` *(view: orders + payment aggregates, `fully_paid` flag)*

### 3) Data Modeling (Gold)

* **Tech:** dbt + dbt-trino adapter
* **Process:** business logic + metric calculation with tests & freshness
* **Models:**

  * `gold.fct_sales_minute` *(incremental)* — minute bucket, GMV, paid order count
* **Quality & Docs:**

  * `models/gold/schema.yml` — not\_null/unique/value checks + column docs
  * `models/sources.yml` — silver sources with `loaded_at_field` + **freshness** rules
  * `models/exposures.yml` — links dashboard to models in lineage
  * *(optional)* `models/gold/vw_sales_last_60min.sql` — BI view filtered to last N minutes

### 4) Orchestration

* **Tech:** Apache Airflow
* **DAGs:**

  * `bronze_streams_raw` — continuous streaming from Kafka to Delta
  * `silver_jobs` — scheduled every 5 minutes
  * `gold_dbt` — runs dbt; dataset-triggered by silver completion; retries + SLA recommended

### 5) Query Engine

* **Tech:** Trino
* **Catalogs:**

  * `delta` — Delta Lake connector (main catalog used)
  * `hive`  — Hive Metastore access (metadata)

---

## Data Flow Details

### Bronze → Silver

**Orders (`silver_orders.py`):**

* Dedupe Kafka messages
* Parse JSON with schema
* Cast fields (`order_id`, `total_amount`, …)
* Add `event_ts`, `event_date`

**Payments (`silver_payments.py`):**

* Dedupe, parse payments
* Convert `amount_cents` → decimal `amount`
* Add synthetic IDs for idempotency

**Enrichment (`silver_enrich.py`):**

* Join orders with payment aggregates
* Compute `paid_amount`, `fully_paid`
* Publish **`silver.order_payments_enriched`** (view)

### Silver → Gold (dbt)

**`models/gold/fct_sales_minute.sql` (incremental):**

* Filters paid orders via `order_payments_enriched`
* Aggregates by `date_trunc('minute', event_ts)`
* Outputs:

  * `minute_bucket` (timestamp)
  * `gmv` (sum of `total_amount`)
  * `paid_orders` (count)
  * `processed_ts`

*(Optional) BI View:* `vw_sales_last_60min` selects last N minutes for dashboards.

---

## Table Schemas

**Bronze**

* `bronze_raw.orders_raw`: `raw_key`, `raw_value`, `topic`, `partition`, `offset`, `kafka_timestamp`, `ingest_ts`
* `bronze_raw.payments_raw`: (same fields)

**Silver**

* `silver.orders_clean`: `order_id`, `user_id`, `items[]`, `currency`, `total_amount`, `status`, `event_ts`, `event_date`, `topic`, `partition`, `offset`
* `silver.payments_clean`: `order_id`, `amount`, `currency`, `status`, `event_ts`, `event_date`, `event_id`, `payment_id`, `topic`, `partition`, `offset`
* `silver.order_payments_enriched` *(view)*: `order_id`, `user_id`, `total_amount`, `paid_amount`, `currency`, `fully_paid`, `order_ts`, `last_payment_ts`, `updated_ts`

**Gold**

* `gold.fct_sales_minute`: `minute_bucket`, `gmv`, `paid_orders`, `processed_ts`

---

## Quick Start

### Build / Test Gold (inside the Airflow container that runs dbt)

```bash
# sanity
dbt debug --profiles-dir /opt/dbt --target dev

# tests & freshness (models + sources)
dbt test  --profiles-dir /opt/dbt --target dev -s fct_sales_minute source:silver

# build the fact (and optionally the view)
dbt build --profiles-dir /opt/dbt --target dev -s fct_sales_minute
# dbt build --profiles-dir /opt/dbt --target dev -s fct_sales_minute vw_sales_last_60min
```

### Accessing with Trino CLI

```bash
docker exec -it trino trino

-- Recent sales
SELECT * FROM delta.gold.fct_sales_minute
ORDER BY minute_bucket DESC
LIMIT 10;

-- Paid orders only
SELECT order_id, total_amount, paid_amount, fully_paid
FROM delta.silver.order_payments_enriched
WHERE fully_paid = true;
```

### Connect Metabase to Trino

1. **Admin → Databases → Add database → Starburst (Trino)**
2. **Display name:** `Trino (delta)`
   **Host:** `trino` *(same Docker network)* or `localhost` *(external)*
   **Port:** `8080`
   **Catalog:** `delta`
   **Schema (optional default):** `gold`
   **Username:** `dbt` *(or read-only user)*
   **Use SSL:** Off (HTTP)
3. **Save**, then **Sync database schema now**.

**Build a chart**

* New Question → **Trino (delta) → gold → `fct_sales_minute`**
* Visualization: **Line**
* X = `minute_bucket` (time), Series = `gmv`, `paid_orders`
* Filter: `minute_bucket` → *Past 60 minutes*
* Save to a dashboard and enable **Auto-refresh** (1–5 min)

*(If you added `vw_sales_last_60min`, use that table directly.)*

---

## Monitoring & Operations

### Airflow DAGs

* `bronze_streams_raw` — long-running
* `silver_jobs` — every 5 minutes
* `gold_dbt` — dataset-triggered by silver (recommended), with:

  * `retries=2`, `retry_delay=2m`
  * `sla=10m`
  * email/Slack on failure

### Data Freshness

* Bronze: near real-time (seconds)
* Silver: 5-minute micro-batches
* Gold: on silver update

**Sanity queries**

```sql
-- Silver should be recent
SELECT max(event_ts) FROM delta.silver.orders_clean;
SELECT max(event_ts) FROM delta.silver.order_payments_enriched;

-- Gold should also be recent
SELECT max(minute_bucket) FROM delta.gold.fct_sales_minute;
```

### Data Quality

* Schema validation in Silver
* dbt tests in Gold (`not_null`, `unique`, value checks)
* Source freshness rules (warn/error if Silver stalls)
* Lineage in **dbt Docs**:

  ```bash
  dbt docs generate --profiles-dir /opt/dbt --target dev
  dbt docs serve    --profiles-dir /opt/dbt --target dev --port 8081 --no-browser
  # open http://localhost:8081
  ```

---

## Configuration

### Key Files

* `profiles.yml` — dbt Trino connection (host, catalog=`delta`, schema, session props)
* `dbt_project.yml` — model paths/config
* `models/sources.yml` — source declarations + freshness
* `models/gold/schema.yml` — tests + docs for gold
* `models/exposures.yml` — dashboard lineage
* Trino: `delta.properties` (Delta connector), `hive.properties` (Hive), `hive-site.xml`

### Environment Variables

* `KAFKA_BOOTSTRAP` — Kafka connection
* `LAKEHOUSE_URI` — MinIO/S3 URI for Delta
* `TOPIC_ORDER_EVENTS`, `TOPIC_PAYMENT_EVENTS` — Kafka topics

---

## Troubleshooting

**Metabase doesn’t show tables**

* Admin → Databases → *Trino (delta)* → **Sync database schema now**
* Confirm host/port reachability from Metabase container

**dbt connection fails with session property error**

* Remove deprecated Trino session properties (e.g., `task_writer_count`)
* Keep only supported options in `profiles.yml`

**SSL warnings in dbt logs**

* You’re using HTTP; safe to ignore. When moving to HTTPS, set:

  * `flags.require_certificate_validation: true` in `dbt_project.yml`
  * `cert: true` in the dbt profile and configure certificates

**Airflow dbt task can’t find the project/profile**

* Ensure Docker mounts map to:

  * `/opt/dbt/ecom_analytics` (project)
  * `/opt/dbt/profiles.yml` (profile)

**Data not updating**

* Check Kafka topics contain events
* Verify Spark jobs are running
* Check Airflow DAG status and logs
* Run the freshness sanity queries above

---

## Performance & Scaling

* Demo config keeps Spark small; increase `spark.executor.memory` and cores for prod
* Partition Silver by `event_date`; periodically **OPTIMIZE/VACUUM** Delta tables
* In Trino, avoid deprecated session props; tune memory only as needed
* Consider managed platforms (Databricks/EMR) for larger workloads
* Add alerting (email/Slack) on Airflow SLAs and dbt test failures

---

## Status

**Phase 2 – Data Engineering:** ✅ Completed
Gold table builds cleanly; Metabase can chart in near real-time over Trino.
