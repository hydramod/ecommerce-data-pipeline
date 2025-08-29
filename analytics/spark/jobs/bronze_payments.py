from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import *
import logging
import sys

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
    try:
        spark = (SparkSession.builder
                 .appName("bronze-payments")
                 .config("spark.sql.shuffle.partitions", "4")
                 .getOrCreate())
        logger.info("Spark session started successfully")

        raw = (spark.readStream
               .format("kafka")
               .option("kafka.bootstrap.servers", "kafka:9092")
               .option("subscribe", "payments.v1")
               # REMOVED: startingOffsets - let checkpoint control offsets
               .load())

        parsed = (raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                   .withColumn("j", from_json(col("value"), PAY_SCHEMA))
                   .select("key", "j.*")
                   .withColumn("event_ts", to_timestamp(col("event_time")))
                   .withColumn("event_date", to_date(col("event_ts"))))

        query = (parsed.writeStream
                 .format("delta")
                 .option("checkpointLocation", "/lake/_chk/bronze/payments")
                 .partitionBy("event_date")
                 .outputMode("append")
                 .start("/lake/bronze/payments"))

        logger.info("Stream started, awaiting termination...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in Spark application: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
