import os, sys
from pyspark.sql import SparkSession, functions as F

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC     = os.getenv("TOPIC_PAYMENT_EVENTS", "payments.events")
LAKE      = os.getenv("LAKEHOUSE_URI", "s3a://delta-lake")

OUT_PATH  = f"{LAKE}/bronze_raw/payments"
CKPT_PATH = f"{LAKE}/checkpoints/bronze_raw/payments"

def main():
    spark = (SparkSession.builder
            .appName("bronze_raw_payments")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.executor.cores", "1")
            .config("spark.cores.max", "1")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .enableHiveSupport()  # Add Hive support
            .getOrCreate())

    # Register schema and table in Hive Metastore
    spark.sql(f"""
      CREATE SCHEMA IF NOT EXISTS bronze_raw
      LOCATION '{LAKE}/warehouse/bronze_raw.db'
    """)

    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS bronze_raw.payments_raw
      USING DELTA
      LOCATION '{OUT_PATH}'
    """)

    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP)
          .option("subscribe", TOPIC)
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", "false")
          .load())

    raw = (df
        .withColumn("raw_key", F.col("key").cast("string"))
        .withColumn("raw_value", F.col("value").cast("string"))
        .withColumn("kafka_timestamp", F.col("timestamp"))
        .withColumn("ingest_ts", F.current_timestamp())
        .select(
            "raw_key","raw_value","topic","partition","offset",
            "kafka_timestamp","ingest_ts","timestampType"
        ))

    # Write to table instead of path
    q = (raw.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CKPT_PATH)
        .trigger(processingTime="10 seconds")
        .toTable("bronze_raw.payments_raw"))  # Changed from .start() to .toTable()

    q.awaitTermination()

if __name__ == "__main__":
    sys.exit(main() or 0)