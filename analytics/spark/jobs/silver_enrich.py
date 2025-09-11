from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max, coalesce, greatest, lit
from delta.tables import DeltaTable

FS = "s3a://delta-lake"
SILVER = f"{FS}/warehouse/silver.db"
ENRICH_LOC = f"{SILVER}/order_payments_enriched"

spark = (SparkSession.builder
        .appName("silver_enriched")
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

orders = (spark.table("silver.orders_clean")
          .select("order_id","user_id","total_amount","currency","event_ts"))

pays_agg = (spark.table("silver.payments_clean")
            .groupBy("order_id")
            .agg(_sum("amount").alias("paid_amount"),
                 _max("event_ts").alias("last_payment_ts"),
                 )
           )

enriched = (orders.alias("o")
            .join(pays_agg.alias("p"), "order_id", "left")
            .select(
                col("order_id"),
                col("o.user_id").alias("user_id"),
                col("o.total_amount").alias("total_amount"),
                col("o.currency").alias("currency"),
                coalesce(col("p.paid_amount"), lit(0.0)).alias("paid_amount"),
                (coalesce(col("p.paid_amount"), lit(0.0)) >= col("o.total_amount")).alias("fully_paid"),
                col("o.event_ts").alias("order_ts"),
                col("p.last_payment_ts").alias("last_payment_ts"),
                greatest(col("o.event_ts"), coalesce(col("p.last_payment_ts"), col("o.event_ts"))).alias("updated_ts")
            ))

if DeltaTable.isDeltaTable(spark, ENRICH_LOC):
    tgt = DeltaTable.forPath(spark, ENRICH_LOC)
    # one row per order_id; upsert safely
    (tgt.alias("t")
        .merge(enriched.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    (enriched.write.format("delta").mode("overwrite").save(ENRICH_LOC))
    spark.sql(f"CREATE TABLE IF NOT EXISTS silver.order_payments_enriched USING DELTA LOCATION '{ENRICH_LOC}'")

spark.stop()
