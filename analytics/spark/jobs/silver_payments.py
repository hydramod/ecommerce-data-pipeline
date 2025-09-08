import os
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def spark_session():
    return (
        SparkSession.builder
        .appName("silver-payments")
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
    try:
        lakehouse = os.getenv("LAKEHOUSE_URI", "s3a://delta-lake")
        bronze_payments = f"{lakehouse}/bronze/payments"
        ckpt = f"{lakehouse}/_chk/silver/payments_clean"
        out  = f"{lakehouse}/silver/payments_clean"

        spark = spark_session()
        logger.info("Spark session started")

        wait_for_delta(spark, bronze_payments)
        src = spark.readStream.format("delta").load(bronze_payments)

        clean = (
            src
            .filter(col("event_ts").isNotNull())
            .withWatermark("event_ts", "30 minutes")
            # prefer payment_id + event_ts as a stable dedupe key
            .dropDuplicates(["payment_id", "event_ts"])
        )

        q = (
            clean.writeStream
            .format("delta")
            .option("checkpointLocation", ckpt)
            .trigger(availableNow=True)      # process new data then exit
            .outputMode("append")
            .start(out)
        )

        logger.info("payments_clean micro-batch running…")
        q.awaitTermination()
        logger.info("payments_clean micro-batch complete")

    except Exception as e:
        logger.error("silver-payments failed", exc_info=True)
        raise

if __name__ == "__main__":
    main()
