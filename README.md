# IST3134 — Steam Review Bias Analysis

Do reviewers who receive a game free of charge recommend it at a higher
rate than those who paid? This project tests that question across 21.7
million Steam reviews, implemented four ways to compare distributed and
single-node execution.

**Group members:** Cheong Jun Yuan (22065585), Tang Yong Zhe (22073464)
**Course:** IST3134 Big Data Analytics in the Cloud

## Dataset

Steam Reviews 2021 — https://www.kaggle.com/datasets/najzeko/steam-reviews-2021
21.7M records, 23 columns, ~8 GB uncompressed CSV. Not included in this
repo; download via the Kaggle CLI (see `scripts/setup.sh`).

## Contents

| Path | Description |
|---|---|
| `hadoop/mapper.py` | Emits (received_for_free, playtime_band) → recommended |
| `hadoop/reducer.py` | Aggregates recommendation rate per group |
| `spark/steam_bias.py` | PySpark DataFrame + Spark SQL implementation |
| `pandas/baseline.py` | Single-node non-big-data baseline |
| `scripts/setup.sh` | Cluster provisioning, dataset staging, subset creation |

## Running

Hadoop Streaming:

    mapred streaming -files mapper.py,reducer.py \
      -input /user/hadoop/steam/steam_reviews.csv \
      -output /user/hadoop/steam/mr-full \
      -mapper "python3 mapper.py" -reducer "python3 reducer.py"

Spark:

    spark-submit steam_bias.py /user/hadoop/steam/steam_reviews.csv

## Environment

Amazon EMR 7.13.0 — 1 × m5.xlarge primary, 3 × m5.xlarge core
(4 vCore / 16 GiB each). Hadoop 3.4.2, Spark 3.5.6, region us-east-1.

## Results summary

- Free-copy reviewers recommend at 88.81% vs 87.43% for purchasers
  (+1.38 pp, χ² = 1158.78, p < 1e-250). Effect survives stratification
  by playtime in all four bands.
- No detectable difference by purchase channel (+0.01 pp, p = 0.556).
- Full corpus: Spark 89.9 s, Hadoop MR 161.3 s, pandas OOM-killed at 7.7 GB.
- Spark `multiLine=true` costs 5.7× (514.2 s vs 89.9 s) because it
  disables input splitting — one partition instead of sixty-one.

Full analysis in the submitted report.
