import os
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver-orders")

def spark_session():
    return (
        SparkSession.builder
        .appName("silver-orders")
        # ---- resource caps (per-app) ----
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        # ---- general ----
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )

def wait_for_delta(spark, path, timeout=None, poll=5):
    """Wait until `path` is an initialized Delta table (has at least one commit)."""
    timeout = int(timeout or os.getenv("SILVER_WAIT_TIMEOUT_SECS", "900"))
    logger.info(f"Waiting for Delta table at {path} (timeout={timeout}s)…")
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

def main():
    lakehouse = os.getenv("LAKEHOUSE_URI", "s3a://delta-lake")
    bronze_orders = f"{lakehouse}/bronze/orders"
    ckpt = f"{lakehouse}/_chk/silver/orders_clean"
    out  = f"{lakehouse}/silver/orders_clean"

    spark = spark_session()
    logger.info("Spark session started")

    # Wait for Bronze to publish its first commit
    wait_for_delta(spark, bronze_orders)

    src = spark.readStream.format("delta").load(bronze_orders)

    clean = (
        src
        .filter(col("event_ts").isNotNull())
        .withWatermark("event_ts", "30 minutes")
        .dropDuplicates(["order_id", "event_ts"])
        .filter(col("total_amount").isNotNull())
    )

    q = (
        clean.writeStream
        .format("delta")
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)  # process what's new then exit
        .outputMode("append")
        .start(out)
    )

    logger.info("orders_clean micro-batch running…")
    q.awaitTermination()
    logger.info("orders_clean micro-batch complete")
    spark.stop()

if __name__ == "__main__":
    main()
