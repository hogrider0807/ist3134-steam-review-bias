#!/usr/bin/env python3
"""
PySpark implementation: Steam review acquisition-channel bias (RQ1-RQ3).

Computes the same aggregations as the Hadoop Streaming job, expressed
declaratively through the DataFrame API. The central RQ2 aggregation is
additionally expressed in Spark SQL to confirm that both surfaces compile
to the same physical plan and return identical results.

The partition count is printed immediately after the read. This matters:
with multiLine=True Spark cannot split the input safely, so the whole file
is processed by a single task. See steam_bias_split.py and Section 4.5.3
of the report.

Usage:
    spark-submit --master yarn --deploy-mode client \
        steam_bias.py hdfs:///user/hadoop/steam/steam_reviews.csv
"""

import sys
import time

from pyspark.sql import SparkSession, functions as F

path = sys.argv[1]
spark = SparkSession.builder.appName("SteamBias").getOrCreate()

t0 = time.time()

df = (spark.read
      .option("header", True)
      .option("multiLine", True)
      .option("quote", '"')
      .option("escape", '"')
      .option("mode", "DROPMALFORMED")
      .csv(path))

print("=== PARTITIONS: %d ===" % df.rdd.getNumPartitions())

clean = (df
         .filter(F.col("recommended").isin("True", "False"))
         .filter(F.col("received_for_free").isin("True", "False"))
         .withColumn("rec", (F.col("recommended") == "True").cast("int"))
         .withColumn("mins", F.col("`author.playtime_at_review`").cast("double"))
         .filter(F.col("mins") >= 0)
         .withColumn("band",
                     F.when(F.col("mins") < 60, "1_under1h")
                      .when(F.col("mins") < 600, "2_1to10h")
                      .when(F.col("mins") < 6000, "3_10to100h")
                      .otherwise("4_over100h"))
         .cache())

total = clean.count()
print("=== CLEAN RECORDS: %d ===" % total)

# ---- RQ2: aggregate -------------------------------------------------------
print("=== RQ2: aggregate ===")
(clean.groupBy("received_for_free")
      .agg(F.count("*").alias("n"),
           F.sum("rec").alias("pos"),
           F.round(F.avg("rec"), 4).alias("rate"))
      .orderBy("received_for_free")
      .show(truncate=False))

# ---- RQ2: stratified by playtime -----------------------------------------
print("=== RQ2: stratified by playtime ===")
(clean.groupBy("band", "received_for_free")
      .agg(F.count("*").alias("n"),
           F.round(F.avg("rec"), 4).alias("rate"))
      .orderBy("band", "received_for_free")
      .show(20, truncate=False))

# ---- RQ1: playtime only ---------------------------------------------------
print("=== RQ1: playtime only ===")
(clean.groupBy("band")
      .agg(F.count("*").alias("n"),
           F.round(F.avg("rec"), 4).alias("rate"))
      .orderBy("band")
      .show(truncate=False))

# ---- RQ3: purchase channel -----------------------------------------------
print("=== RQ3: steam_purchase ===")
(clean.filter(F.col("steam_purchase").isin("True", "False"))
      .groupBy("steam_purchase")
      .agg(F.count("*").alias("n"),
           F.round(F.avg("rec"), 4).alias("rate"))
      .orderBy("steam_purchase")
      .show(truncate=False))

# ---- Same aggregation via Spark SQL --------------------------------------
print("=== Spark SQL equivalent ===")
clean.createOrReplaceTempView("reviews")
spark.sql("""
    SELECT received_for_free,
           COUNT(*)              AS n,
           ROUND(AVG(rec), 4)    AS rate
    FROM reviews
    GROUP BY received_for_free
    ORDER BY received_for_free
""").show(truncate=False)

print("=== TOTAL SECONDS: %.1f ===" % (time.time() - t0))
spark.stop()
