import os
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        lakehouse = os.getenv("LAKEHOUSE_URI", "s3a://delta-lake")
        bronze_payments = f"{lakehouse}/bronze/payments"
        chk = f"{lakehouse}/_chk/silver/payments_clean"
        out = f"{lakehouse}/silver/payments_clean"

        spark = (
            SparkSession.builder
            .appName("silver-payments")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        logger.info("Spark session started successfully")

        bronze = spark.readStream.format("delta").load(bronze_payments)

        clean = (
            bronze
            .withWatermark("event_ts", "30 minutes")
            .dropDuplicates(["event_id"])
        )

        query = (
            clean.writeStream
            .format("delta")
            .option("checkpointLocation", chk)
            .outputMode("append")
            .start(out)
        )

        logger.info("Stream started, awaiting termination.")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in Spark application: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
