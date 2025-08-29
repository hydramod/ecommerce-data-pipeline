from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, current_timestamp
from delta.tables import DeltaTable
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upsert_to_enriched_table(microBatchDF, batchId):
    """
    foreachBatch function to perform UPSERT using Delta Lake MERGE
    """
    logger.info(f"Processing batchId: {batchId}")
    
    # Create or get the target Delta table
    target_path = "/lake/silver/order_payments_enriched"
    if DeltaTable.isDeltaTable(spark, target_path):
        target_delta_table = DeltaTable.forPath(spark, target_path)
    else:
        # First run - create the table by writing the initial data
        microBatchDF.write.format("delta").save(target_path)
        return
    
    # Merge logic - update if exists, insert if not
    # Using order_id + event_ts as unique key (adjust based on your needs)
    merge_condition = "target.order_id = source.order_id AND target.event_ts = source.event_ts"
    
    target_delta_table.alias("target").merge(
        microBatchDF.alias("source"), 
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def main():
    try:
        global spark
        spark = (SparkSession.builder
                 .appName("silver-enrich")
                 .config("spark.sql.shuffle.partitions", "4")
                 .getOrCreate())
        logger.info("Spark session started successfully")

        orders = spark.readStream.format("delta").load("/lake/silver/orders_clean")
        payments = spark.readStream.format("delta").load("/lake/silver/payments_clean")

        ow = orders.withWatermark("event_ts", "30 minutes")
        pw = payments.withWatermark("event_ts", "30 minutes")

        joined = ow.join(
            pw,
            expr("""
                orders_clean.order_id = payments_clean.order_id
                AND payments_clean.event_ts BETWEEN orders_clean.event_ts - interval 1 day
                AND orders_clean.event_ts + interval 2 days
            """),
            "leftOuter"
        )

        # Add processing timestamp for auditing
        enriched = joined.withColumn("processed_ts", current_timestamp())

        query = (enriched.writeStream
                 .format("delta")
                 .option("checkpointLocation", "/lake/_chk/silver/order_payments_enriched")
                 .foreachBatch(upsert_to_enriched_table)
                 .outputMode("update")  # Changed to update for foreachBatch
                 .start())
        
        logger.info("Stream started, awaiting termination...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in Spark application: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
