#analytics\airflow\dags\rt_pipeline.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ORDERS_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("sku", StringType()),
        StructField("qty", IntegerType()),
        StructField("price", DoubleType())
    ]))),
    StructField("currency", StringType()),
    StructField("total_amount", DoubleType()),
    StructField("status", StringType()),
    StructField("event_time", StringType()),
    StructField("ingest_ts", StringType())
])

def main():
    # ---- Env wiring ----
    lakehouse = os.getenv("LAKEHOUSE_URI", "s3a://delta-lake")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    topic_orders = os.getenv("TOPIC_ORDER_EVENTS", "orders.events")

    sink_path = f"{lakehouse}/bronze/orders"
    chk_path = f"{lakehouse}/_chk/bronze/orders"

    try:
        spark = (SparkSession.builder
                 .appName("bronze-orders")
                 .config("spark.sql.shuffle.partitions", "4")
                 .getOrCreate())
        logger.info("Spark session started")

        raw = (spark.readStream
               .format("kafka")
               .option("kafka.bootstrap.servers", kafka_bootstrap)
               .option("subscribe", topic_orders)
               .load())

        parsed = (raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                    .withColumn("j", from_json(col("value"), ORDERS_SCHEMA))
                    .select("key", "j.*")
                    .withColumn("event_ts", to_timestamp(col("event_time")))
                    .withColumn("event_date", to_date(col("event_ts"))))

        query = (parsed.writeStream
                 .format("delta")
                 .option("checkpointLocation", chk_path)
                 .partitionBy("event_date")
                 .outputMode("append")
                 .start(sink_path))

        logger.info(f"Streaming to {sink_path} with checkpoints at {chk_path}")
        query.awaitTermination()

    except Exception as e:
        logger.exception(f"Fatal error in bronze-orders: {e}")
        raise

if __name__ == "__main__":
    main()
