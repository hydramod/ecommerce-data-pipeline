from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        spark = (SparkSession.builder
                 .appName("silver-orders")
                 .config("spark.sql.shuffle.partitions", "4")
                 .getOrCreate())
        logger.info("Spark session started successfully")

        bronze = spark.readStream.format("delta").load("/lake/bronze/orders")

        clean = (bronze
                 .withWatermark("event_ts", "30 minutes")
                 .dropDuplicates(["event_id"])
                 .filter(col("total_amount").isNotNull()))

        query = (clean.writeStream
                 .format("delta")
                 .option("checkpointLocation", "/lake/_chk/silver/orders_clean")
                 .outputMode("append")
                 .start("/lake/silver/orders_clean"))

        logger.info("Stream started, awaiting termination...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in Spark application: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
