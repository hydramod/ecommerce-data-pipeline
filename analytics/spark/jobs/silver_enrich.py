import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, current_timestamp
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upsert_to_enriched_table(micro_batch_df, batch_id, lakehouse_base: str):
    """
    foreachBatch function to perform UPSERT using Delta Lake MERGE
    """
    if micro_batch_df is None:
        return

    spark = micro_batch_df.sparkSession
    target_path = f"{lakehouse_base}/silver/order_payments_enriched"

    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        # Key: order_id + event_ts (adjust to your business key if needed)
        merge_cond = "t.order_id = s.order_id AND t.event_ts = s.event_ts"
        (target.alias("t")
               .merge(micro_batch_df.alias("s"), merge_cond)
               .whenMatchedUpdateAll()
               .whenNotMatchedInsertAll()
               .execute())
    else:
        # Initial create
        micro_batch_df.write.format("delta").mode("append").save(target_path)

def main():
    try:
        lakehouse = os.getenv("LAKEHOUSE_URI", "s3a://delta-lake")
        orders_clean = f"{lakehouse}/silver/orders_clean"
        payments_clean = f"{lakehouse}/silver/payments_clean"
        chk = f"{lakehouse}/_chk/silver/order_payments_enriched"

        spark = (
            SparkSession.builder
            .appName("silver-enrich")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        logger.info("Spark session started successfully")

        o = spark.readStream.format("delta").load(orders_clean).alias("o")
        p = spark.readStream.format("delta").load(payments_clean).alias("p")

        ow = o.withWatermark("event_ts", "30 minutes")
        pw = p.withWatermark("event_ts", "30 minutes")

        joined = ow.join(
            pw,
            expr("""
                p.order_id = o.order_id
                AND p.event_ts BETWEEN o.event_ts - interval 1 day
                                   AND o.event_ts + interval 2 days
            """),
            "leftOuter",
        ).withColumn("processed_ts", current_timestamp())

        query = (
            joined.writeStream
            .option("checkpointLocation", chk)
            # use foreachBatch so we can MERGE into the Delta target
            .foreachBatch(lambda df, bid: upsert_to_enriched_table(df, bid, lakehouse))
            .outputMode("update")
            .start()
        )

        logger.info("Stream started, awaiting termination...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in Spark application: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
