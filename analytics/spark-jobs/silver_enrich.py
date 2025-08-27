from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

LAKE_PATH = os.getenv("LAKE_PATH", "/lake")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", f"{LAKE_PATH}/_chk")

def spark_session(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_ROOT}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def main():
    spark = spark_session("rt-silver-enrich")

    orders = (spark.readStream
        .format("delta")
        .load(f"{LAKE_PATH}/bronze/orders")
        .withColumnRenamed("event_ts", "o_event_ts")
        .withWatermark("o_event_ts", "30 minutes")
    )

    payments = (spark.readStream
        .format("delta")
        .load(f"{LAKE_PATH}/bronze/payments")
        .withColumnRenamed("event_ts", "p_event_ts")
        .withWatermark("p_event_ts", "30 minutes")
    )

    joined = (orders.join(
        payments,
        on=orders.order_id == payments.order_id,
        how="leftOuter"
    ))

    (joined.writeStream
        .format("delta")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver/order_payments_enriched")
        .outputMode("append")
        .start(f"{LAKE_PATH}/silver/order_payments_enriched")
        .awaitTermination())

if __name__ == "__main__":
    main()