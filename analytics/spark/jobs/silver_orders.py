from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder
         .appName("silver-orders")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

bronze = spark.readStream.format("delta").load("/lake/bronze/orders")

clean = (bronze
         .withWatermark("event_ts", "30 minutes")
         .dropDuplicates(["event_id"])
         .filter(col("total_amount").isNotNull()))

(clean.writeStream
 .format("delta")
 .option("checkpointLocation", "/lake/_chk/silver/orders_clean")
 .outputMode("append")
 .start("/lake/silver/orders_clean")
 .awaitTermination())
