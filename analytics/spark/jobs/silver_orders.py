from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date, row_number
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
from pyspark.sql import Window
from delta.tables import DeltaTable

FS = "s3a://delta-lake"
SILVER_LOC = f"{FS}/warehouse/silver.db"
ORDERS_LOC = f"{SILVER_LOC}/orders_clean"

orders_schema = StructType([
    StructField("event_type", StringType()),
    StructField("event_version", StringType()),
    StructField("trace_id", StringType()),
    StructField("order_id", StringType()),   # cast to string regardless of source type
    StructField("user_id", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("product_id", IntegerType()),
        StructField("qty", IntegerType()),
        StructField("price", DoubleType())
    ]))),
    StructField("currency", StringType()),
    StructField("total_amount", DoubleType()),
    StructField("status", StringType()),
    StructField("event_time", StringType()),
    StructField("shipping", StructType([])), # ignored for now
    StructField("event_id", StringType()),
    StructField("ingest_ts", StringType()),
])

spark = (SparkSession.builder
        .appName("silver_orders")
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

raw = spark.table("bronze_raw.orders_raw")

# Deduplicate by Kafka identity
w = Window.partitionBy("topic","partition","offset").orderBy(col("kafka_timestamp").desc())
dedup = (raw.withColumn("rn", row_number().over(w))
             .filter(col("rn")==1)
             .drop("rn"))

parsed = (dedup
    .select("topic","partition","offset","kafka_timestamp",
            from_json(col("raw_value"), orders_schema).alias("j"))
    .select(
        col("topic"), col("partition"), col("offset"), col("kafka_timestamp"),
        col("j.event_type").alias("event_type"),
        col("j.order_id").cast("string").alias("order_id"),
        col("j.user_id").alias("user_id"),
        col("j.items").alias("items"),
        col("j.currency").alias("currency"),
        col("j.total_amount").cast("double").alias("total_amount"),
        col("j.status").alias("status"),
        col("j.event_time").alias("event_time"),
        col("j.event_id").alias("event_id"),
        col("j.ingest_ts").alias("ingest_ts")
    )
    .withColumn("event_ts", to_timestamp("event_time"))
    .withColumn("event_date", to_date("event_ts"))
)

if DeltaTable.isDeltaTable(spark, ORDERS_LOC):
    tgt = DeltaTable.forPath(spark, ORDERS_LOC)
    (tgt.alias("t")
        .merge(parsed.alias("s"),
               "t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    (parsed.write.format("delta")
           .mode("overwrite")
           .option("overwriteSchema","true")
           .save(ORDERS_LOC))
    spark.sql(f"CREATE TABLE IF NOT EXISTS silver.orders_clean USING DELTA LOCATION '{ORDERS_LOC}'")

spark.stop()
