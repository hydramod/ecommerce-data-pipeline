from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date, row_number, sha2, concat_ws, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql import Window
from delta.tables import DeltaTable

FS = "s3a://delta-lake"
SILVER_LOC = f"{FS}/warehouse/silver.db"
PAY_LOC = f"{SILVER_LOC}/payments_clean"

payments_schema = StructType([
    StructField("type", StringType()),          # e.g. "payment.succeeded"
    StructField("order_id", LongType()),
    StructField("amount_cents", LongType()),
    StructField("currency", StringType()),
    StructField("user_email", StringType())
])

spark = (SparkSession.builder
        .appName("silver_payments")
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

raw = spark.table("bronze_raw.payments_raw")

w = Window.partitionBy("topic","partition","offset").orderBy(col("kafka_timestamp").desc())
dedup = (raw.withColumn("rn", row_number().over(w))
             .filter(col("rn")==1)
             .drop("rn"))

parsed = (dedup
    .select("topic","partition","offset","kafka_timestamp",
            from_json(col("raw_value"), payments_schema).alias("j"))
    .select(
        col("topic"), col("partition"), col("offset"), col("kafka_timestamp"),
        col("j.order_id").cast("string").alias("order_id"),
        (col("j.amount_cents").cast("double")/ lit(100.0)).alias("amount"),
        col("j.currency").alias("currency"),
        # fields to keep parity with earlier schema
        col("j.type").alias("status"),    # simple mapping; you can normalize to SUCCEEDED/PENDING later
        lit(None).cast("string").alias("method"),
        lit(None).cast("string").alias("event_time"),
        lit(None).cast("string").alias("ingest_ts"),
        # synthetic IDs for idempotency & lineage
        sha2(concat_ws(":", col("topic"), col("partition").cast("string"), col("offset").cast("string")), 256).alias("event_id"),
        sha2(concat_ws(":", col("topic"), col("partition").cast("string"), col("offset").cast("string")), 256).alias("payment_id")
    )
    .withColumn("event_ts", col("kafka_timestamp"))
    .withColumn("event_date", to_date("event_ts"))
)

if DeltaTable.isDeltaTable(spark, PAY_LOC):
    tgt = DeltaTable.forPath(spark, PAY_LOC)
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
           .save(PAY_LOC))
    spark.sql(f"CREATE TABLE IF NOT EXISTS silver.payments_clean USING DELTA LOCATION '{PAY_LOC}'")

spark.stop()
