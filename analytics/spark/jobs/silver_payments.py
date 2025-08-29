from pyspark.sql import SparkSession
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        spark = (SparkSession.builder
                 .appName("silver-payments")
                 .config("spark.sql.shuffle.partitions", "4")
                 .getOrCreate())
        logger.info("Spark session started successfully")

        bronze = spark.readStream.format("delta").load("/lake/bronze/payments")

        clean = (bronze
                 .withWatermark("event_ts", "30 minutes")
                 .dropDuplicates(["event_id"]))

        query = (clean.writeStream
                 .format("delta")
                 .option("checkpointLocation", "/lake/_chk/silver/payments_clean")
                 .outputMode("append")
                 .start("/lake/silver/payments_clean"))

        logger.info("Stream started, awaiting termination...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in Spark application: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
