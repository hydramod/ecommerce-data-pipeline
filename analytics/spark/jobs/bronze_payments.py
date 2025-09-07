import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PAY_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("payment_id", StringType()),
    StructField("order_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("method", StringType()),
    StructField("status", StringType()),
    StructField("event_time", StringType()),
    StructField("ingest_ts", StringType())
])

def main():
    # ---- Env wiring ----
    lakehouse = os.getenv("LAKEHOUSE_URI", "s3a://delta-lake")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    topic_payments = os.getenv("TOPIC_PAYMENT_EVENTS", "payments.events")

    sink_path = f"{lakehouse}/bronze/payments"
    chk_path = f"{lakehouse}/_chk/bronze/payments"

    try:
        spark = (
            SparkSession.builder
            .appName("bronze-payments")
            # ---- resource caps (per-app) ----
            .config("spark.executor.cores", "1")
            .config("spark.cores.max", "1")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        logger.info("Spark session started")

        raw = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("subscribe", topic_payments)
            .load()
        )

        parsed = (
            raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
               .withColumn("j", from_json(col("value"), PAY_SCHEMA))
               .select("key", "j.*")
               .withColumn("event_ts", to_timestamp(col("event_time")))
               .withColumn("event_date", to_date(col("event_ts")))
        )

        query = (
            parsed.writeStream
            .format("delta")
            .option("checkpointLocation", chk_path)
            .partitionBy("event_date")
            .outputMode("append")
            .start(sink_path)
        )

        logger.info(f"Streaming to {sink_path} with checkpoints at {chk_path}")
        query.awaitTermination()

    except Exception as e:
        logger.exception(f"Fatal error in bronze-payments: {e}")
        raise

if __name__ == "__main__":
    main()
