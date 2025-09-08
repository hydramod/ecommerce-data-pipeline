#!/usr/bin/env python3
import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number, coalesce
from pyspark.sql.window import Window
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_enrich")

LAKE = os.getenv("LAKEHOUSE_URI", "s3a://delta-lake")
ORDERS_SILVER   = f"{LAKE}/silver/orders_clean"
PAYMENTS_SILVER = f"{LAKE}/silver/payments_clean"
TARGET          = f"{LAKE}/silver/order_payments_enriched"

def spark_session():
    return (
        SparkSession.builder
        .appName("silver_enrich")
        # ---- resource caps (per-app) ----
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        # ---- Delta / general ----
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def path_ready(spark: SparkSession, path: str) -> bool:
    try:
        # if it’s a Delta table with any metadata, loading doesn’t throw
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False

def wait_for_delta(spark, path, timeout=600, poll=5):
    timeout = int(timeout or os.getenv("SILVER_WAIT_TIMEOUT_SECS", "900"))
    logger.info(f"Waiting for Delta table at {path} (timeout={timeout}s)...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            if DeltaTable.isDeltaTable(spark, path):
                logger.info(f"Delta table is ready: {path}")
                return
        except Exception:
            pass
        time.sleep(poll)
    raise TimeoutError(f"Delta table not found at {path} after {timeout}s")

def df_empty(df) -> bool:
    return df.rdd.isEmpty()

def main():
    spark = spark_session()

    try:
        # 1) Wait until silver inputs exist (created by your silver batch jobs)
        wait_for_delta(spark, ORDERS_SILVER, timeout=600)
        wait_for_delta(spark, PAYMENTS_SILVER, timeout=600)

        # 2) Load inputs (schema produced by your silver jobs)
        orders = (
            spark.read.format("delta").load(ORDERS_SILVER)
            .select(
                col("order_id").cast("string"),
                col("user_id").alias("customer_id").cast("string"),
                col("event_ts").cast("timestamp").alias("order_event_ts"),
                col("total_amount").cast("double"),
            )
        )

        payments = (
            spark.read.format("delta").load(PAYMENTS_SILVER)
            .select(
                col("payment_id").cast("string"),
                col("order_id").cast("string"),
                col("event_ts").cast("timestamp").alias("payment_event_ts"),
                col("amount").cast("double"),
                col("status").cast("string"),
            )
        )

        if df_empty(orders):
            logger.info("orders_clean is empty; nothing to enrich. Exiting 0.")
            spark.stop(); return
        if df_empty(payments):
            logger.info("payments_clean is empty; nothing to enrich. Exiting 0.")
            spark.stop(); return

        # 3) Enrichment: latest payment per order
        w = Window.partitionBy("order_id").orderBy(col("payment_event_ts").desc())
        latest_pay = (
            payments
            .withColumn("rn", row_number().over(w))
            .where(col("rn") == 1)
            .drop("rn")
        )

        enriched = (
            orders.alias("o")
            .join(latest_pay.alias("p"), on="order_id", how="left")
            .select(
                col("o.order_id"),
                col("o.customer_id"),
                col("o.total_amount").alias("order_total_amount"),
                col("o.order_event_ts"),
                col("p.payment_id"),
                col("p.amount").alias("payment_amount"),
                col("p.status").alias("payment_status"),
                col("p.payment_event_ts"),
                # single canonical event_ts for the row (prefer payment time)
                coalesce(col("p.payment_event_ts"), col("o.order_event_ts")).alias("event_ts"),
                current_timestamp().alias("processed_at"),
            )
        )

        # 4) Upsert into Delta target
        if DeltaTable.isDeltaTable(spark, TARGET):
            tgt = DeltaTable.forPath(spark, TARGET)
            (
                tgt.alias("t")
                .merge(
                    enriched.alias("s"),
                    # MERGE key: order_id + payment_id (null-safe)
                    "t.order_id <=> s.order_id AND t.payment_id <=> s.payment_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("MERGE complete into %s", TARGET)
        else:
            enriched.write.format("delta").mode("overwrite").save(TARGET)
            logger.info("Created target table at %s", TARGET)

    except Exception:
        logger.error("silver-enrich failed", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()