from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = (SparkSession.builder
         .appName("silver-enrich")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

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

(joined.writeStream
 .format("delta")
 .option("checkpointLocation", "/lake/_chk/silver/order_payments_enriched")
 .outputMode("append")
 .start("/lake/silver/order_payments_enriched")
 .awaitTermination())
