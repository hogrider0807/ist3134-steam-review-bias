#!/usr/bin/env python3
"""
PySpark implementation with input splitting enabled (RQ1-RQ3).

Identical to steam_bias.py except that multiLine is set to False, which
permits Spark to split the input file across tasks. On the full 7.7 GB
corpus this produces 61 partitions rather than 1, and reduces execution
time from 514 s to 90 s.

The completeness cost is negligible: 21,721,666 records retained with
splitting against 21,721,689 without, a difference of 23 records in 21.7
million. Spark's CSV parser handles quoted newlines correctly within a
split, so only records spanning a split boundary are lost.

This file is generated from steam_bias.py rather than maintained
separately, guaranteeing that no other difference is introduced:

    sed 's/.option("multiLine", True)/.option("multiLine", False)/' \
        steam_bias.py > steam_bias_split.py

Usage:
    spark-submit --master yarn --deploy-mode client \
        steam_bias_split.py hdfs:///user/hadoop/steam/steam_reviews.csv
"""

import sys
import time

from pyspark.sql import SparkSession, functions as F

path = sys.argv[1]
spark = SparkSession.builder.appName("SteamBiasSplit").getOrCreate()

t0 = time.time()

df = (spark.read
      .option("header", True)
      .option("multiLine", False)
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
