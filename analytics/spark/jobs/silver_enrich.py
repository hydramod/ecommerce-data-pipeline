#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F

DB = "silver"

def spark_session(app="silver_jobs"):
    return (
        SparkSession.builder
        .appName(app)
        .enableHiveSupport()
        # ---- resource caps (per-app) ----
        .config("spark.executor.cores", "1")   # 1 core per executor
        .config("spark.cores.max", "1")        # 1 core total for the app
        .config("spark.executor.instances", "1")  # (belt & braces) single executor
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def main():
    spark = spark_session("silver_enrich")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"ALTER DATABASE {DB} SET LOCATION 's3a://delta-lake/warehouse/{DB}.db'")

    orders   = spark.table(f"{DB}.orders_clean")
    payments = spark.table(f"{DB}.payments_clean")

    # Choose a join key. Prefer 'order_id' if present on both.
    join_cols = set(orders.columns) & set(payments.columns)
    if "order_id" in join_cols:
        enriched = (
            orders.alias("o")
            .join(payments.alias("p"), on="order_id", how="left")
        )
    else:
        # Fallback: leave as orders only (adjust if your schema differs)
        enriched = orders

    # Example derived fields; keep your real logic if you had it:
    if "amount" in payments.columns:
        totals = payments.groupBy("order_id").agg(F.sum("amount").alias("paid_amount"))
        enriched = (
            orders.alias("o")
            .join(totals.alias("t"), on="order_id", how="left")
            .withColumn("paid_amount", F.coalesce(F.col("paid_amount"), F.lit(0.0)))
        )

    target = f"{DB}.order_payments_enriched"
    (enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema","true")
        .saveAsTable(target))

    spark.stop()

if __name__ == "__main__":
    main()
