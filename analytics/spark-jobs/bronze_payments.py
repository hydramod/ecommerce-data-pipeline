from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import *
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
    spark = spark_session("rt-bronze-payments")

    payments_schema = StructType([
        StructField("event_id", StringType()),
        StructField("payment_id", StringType()),
        StructField("order_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("method", StringType()),
        StructField("status", StringType()),
        StructField("event_time", StringType()),
        StructField("ingest_ts", StringType()),
    ])

    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
        .option("subscribe", "payments.v1")
        .option("startingOffsets", "latest")
        .load()
    )

    df = (
        df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
             .withColumn("json", from_json(col("value"), payments_schema))
             .select("key", "json.*")
             .withColumn("event_ts", to_timestamp(col("event_time")))
             .withColumn("event_date", to_date(col("event_ts")))
    )

    (df.writeStream
       .format("delta")
       .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze/payments")
       .partitionBy("event_date")
       .outputMode("append")
       .start(f"{LAKE_PATH}/bronze/payments")
       .awaitTermination())

if __name__ == "__main__":
    main()