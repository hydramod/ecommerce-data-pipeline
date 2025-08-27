from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("silver-payments")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

bronze = spark.readStream.format("delta").load("/lake/bronze/payments")

clean = (bronze
         .withWatermark("event_ts", "30 minutes")
         .dropDuplicates(["event_id"]))

(clean.writeStream
 .format("delta")
 .option("checkpointLocation", "/lake/_chk/silver/payments_clean")
 .outputMode("append")
 .start("/lake/silver/payments_clean")
 .awaitTermination())
