#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession, functions as F

LAKE = os.getenv("LAKEHOUSE_URI", "s3a://delta-lake")
DB   = "silver"  # Hive database/schema

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
    spark = spark_session("silver_orders")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"ALTER DATABASE {DB} SET LOCATION 's3a://delta-lake/warehouse/{DB}.db'")

    # Source: bronze Delta path
    orders_src = f"{LAKE}/bronze/orders"
    df = spark.read.format("delta").load(orders_src)

    # Minimal cleanup example
    df = df.dropna(how="all")
    if "order_id" in df.columns:
        df = df.dropDuplicates(["order_id"])
    else:
        df = df.dropDuplicates()

    # Write as a managed Delta table registered in Hive
    target = f"{DB}.orders_clean"
    (df.write
       .format("delta")
       .mode("overwrite")                # idempotent for dev
       .option("overwriteSchema", "true")
       .saveAsTable(target))

    spark.stop()

if __name__ == "__main__":
    main()
