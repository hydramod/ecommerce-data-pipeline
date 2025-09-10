#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession, functions as F

LAKE = os.getenv("LAKEHOUSE_URI", "s3a://delta-lake")
DB   = "silver"

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
    spark = spark_session("silver_payments")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"ALTER DATABASE {DB} SET LOCATION 's3a://delta-lake/warehouse/{DB}.db'")

    payments_src = f"{LAKE}/bronze/payments"
    df = spark.read.format("delta").load(payments_src)

    df = df.dropna(how="all")
    # Dedup if you have a natural key
    key = "payment_id" if "payment_id" in df.columns else None
    if key:
        df = df.dropDuplicates([key])
    else:
        df = df.dropDuplicates()

    target = f"{DB}.payments_clean"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(target))

    spark.stop()

if __name__ == "__main__":
    main()
