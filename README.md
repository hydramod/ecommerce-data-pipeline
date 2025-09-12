# Ecommerce Microservices - Analytics Pipeline

This document describes the real-time analytics pipeline built on top of the e-commerce microservices. The pipeline processes event data from Kafka, transforms it through multiple layers, and makes it available for analytics and reporting.

## Architecture Overview

The analytics pipeline follows a medallion architecture with Bronze (raw), Silver (cleaned), and Gold (enriched) layers:

```
Kafka Topics → Spark Streaming (Bronze) → Spark Batch (Silver) → dbt (Gold) → Trino (Query)
```

## Components

### 1. Data Ingestion (Bronze Layer)
- **Technology**: Apache Spark Streaming
- **Sources**: Kafka topics (`orders.events`, `payments.events`)
- **Destination**: Delta Lake in MinIO (S3-compatible storage)
- **Tables**: 
  - `bronze_raw.orders_raw` - Raw order events
  - `bronze_raw.payments_raw` - Raw payment events

### 2. Data Processing (Silver Layer)
- **Technology**: Apache Spark Batch
- **Process**: Data cleaning, validation, and basic enrichment
- **Tables**:
  - `silver.orders_clean` - Cleaned order data
  - `silver.payments_clean` - Cleaned payment data
  - `silver.order_payments_enriched` - View joining orders with payment information

### 3. Data Modeling (Gold Layer)
- **Technology**: dbt with Trino adapter
- **Process**: Business logic and metric calculation
- **Tables**:
  - `gold.fct_sales_minute` - Minute-level sales aggregates

### 4. Orchestration
- **Technology**: Apache Airflow
- **DAGs**:
  - `bronze_streams_raw` - Continuous streaming from Kafka to Delta
  - `silver_jobs` - Batch processing every 5 minutes
  - `gold_dbt` - Triggered when silver processing completes

### 5. Query Engine
- **Technology**: Trino
- **Purpose**: SQL interface to Delta Lake tables
- **Catalogs**: 
  - `delta` - Delta Lake connector
  - `hive` - Hive connector (for metadata)

## Data Flow

### Bronze → Silver Processing

**Orders Processing** (`silver_orders.py`):
- Deduplicates Kafka messages
- Parses JSON payload with schema validation
- Extracts and casts fields (order_id, user_id, total_amount, etc.)
- Adds event timestamps and dates

**Payments Processing** (`silver_payments.py`):
- Deduplicates Kafka messages  
- Parses payment events
- Converts amount_cents to decimal amount
- Adds synthetic IDs for idempotency

**Data Enrichment** (`silver_enrich.py`):
- Joins orders with payment aggregates
- Calculates paid amounts and fully_paid status
- Creates business-ready enriched view

### Silver → Gold Processing

**dbt Models** (`fct_sales_minute.sql`):
- Incremental model that processes new data only
- Aggregates paid orders by minute
- Calculates GMV (Gross Merchandise Value) and order counts
- Uses Trino as the query engine

## Tables Schema

### Bronze Layer
**bronze_raw.orders_raw**:
- raw_key, raw_value (string)
- topic, partition, offset
- kafka_timestamp, ingest_ts

**bronze_raw.payments_raw**:
- Same structure as orders_raw

### Silver Layer  
**silver.orders_clean**:
- order_id, user_id, items (array of structs)
- currency, total_amount, status
- event_ts, event_date
- topic, partition, offset (for lineage)

**silver.payments_clean**:
- order_id, amount, currency, status
- event_ts, event_date  
- event_id, payment_id (synthetic keys)
- topic, partition, offset

**silver.order_payments_enriched** (view):
- order_id, user_id, total_amount, paid_amount
- currency, fully_paid (boolean)
- order_ts, last_payment_ts, updated_ts

### Gold Layer
**gold.fct_sales_minute**:
- minute_bucket (timestamp)
- gmv (total sales amount)
- paid_orders (order count)
- processed_ts (when the record was created)

## Accessing the Data

### Using Trino CLI
```bash
docker exec -it trino trino

-- Query recent sales
SELECT * FROM delta.gold.fct_sales_minute ORDER BY minute_bucket DESC LIMIT 10;

-- Check order payment status
SELECT order_id, total_amount, paid_amount, fully_paid 
FROM delta.silver.order_payments_enriched 
WHERE fully_paid = true;
```

### Using dbt
```bash
# Run dbt models
docker exec -it airflow-worker dbt run --profiles-dir /opt/dbt --target dev --select fct_sales_minute

# Test dbt models  
docker exec -it airflow-worker dbt test --profiles-dir /opt/dbt --target dev
```

## Monitoring and Operations

### Airflow DAGs
- **bronze_streams_raw**: Always-running streaming jobs
- **silver_jobs**: Scheduled every 5 minutes
- **gold_dbt**: Dataset-triggered (when silver data is updated)

### Data Freshness
- Bronze: Near real-time (seconds latency)
- Silver: 5-minute batches  
- Gold: Triggered by silver updates

### Data Quality
- Schema validation during silver processing
- dbt tests for gold layer models
- Data lineage tracking through topic/partition/offset

## Configuration Files

### Key Configuration Files
- `hive-site.xml` - Hive Metastore configuration
- `delta.properties` - Trino Delta Lake connector
- `hive.properties` - Trino Hive connector  
- `profiles.yml` - dbt Trino connection
- `sources.yml` - dbt source definitions

### Environment Variables
- `KAFKA_BOOTSTRAP` - Kafka connection string
- `LAKEHOUSE_URI` - MinIO/S3 location for Delta Lake
- `TOPIC_ORDER_EVENTS` - Kafka topic for orders
- `TOPIC_PAYMENT_EVENTS` - Kafka topic for payments

## Troubleshooting

### Common Issues

1. **Hive Metastore Connection Issues**
   - Check PostgreSQL is running
   - Verify `hive-site.xml` configuration

2. **Delta Table Access Issues**
   - Confirm MinIO credentials in `delta.properties`
   - Check table exists in Hive Metastore

3. **dbt Connection Issues**
   - Validate Trino is accessible
   - Check `profiles.yml` configuration

4. **Data Not Appearing**
   - Check Kafka topics have messages
   - Verify Spark jobs are running
   - Monitor Airflow DAG status

### Useful Commands

```bash
# Check Kafka topics
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092

# Monitor Spark jobs
docker logs spark-master

# Check Airflow DAG status
docker exec -it airflow-worker airflow dags list

# Test Trino connection
docker exec -it trino trino --execute "SELECT 1"
```

## Performance Considerations

- Spark is configured with minimal resources for demo purposes
- Increase `spark.executor.memory` and `spark.executor.cores` for production
- Consider partitioning silver tables by `event_date` for larger datasets
- Use Delta Lake optimizations (Z-order, data skipping) for better query performance

## Scaling

For production workloads:
- Increase Spark resources and parallelism
- Use Delta Lake's built-in optimizations
- Consider using Databricks or EMR for managed Spark
- Add monitoring and alerting for data quality issues
- Implement data retention policies