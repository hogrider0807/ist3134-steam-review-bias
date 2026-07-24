#!/usr/bin/env python3
"""
Single-node pandas baseline (non-big-data comparison).

Performs the same aggregations as the Hadoop and Spark implementations on
one machine, with no distribution. Serves as the control against which the
distributed engines are compared.

Expected behaviour by input size, measured on an m5.xlarge node with 16 GB
of memory:

    805 MB   22.2 s   fastest of all four implementations
    1.6 GB   40.6 s   near parity with Hadoop and Spark
    7.7 GB   ---      terminated by the OOM killer after 2 min 11 s

The failure at full scale is the expected result, not a defect. A
mixed-type CSV expands substantially when materialised as a DataFrame
because Python object overhead applies to every string value.

Note on input: pandas' C parser raises ParserError on a file that ends
inside a quoted field. Benchmark subsets created by plain line truncation
must have a trailing margin removed first:

    head -n 5000001 steam_reviews.csv | head -n -50 > s5m_fix.csv

Usage:
    python3 baseline.py /mnt1/steam/steam_reviews.csv
"""

import sys
import time

import pandas as pd

path = sys.argv[1]
t0 = time.time()

df = pd.read_csv(path, low_memory=False)
print("Loaded %d rows in %.1fs" % (len(df), time.time() - t0))

clean = df[df["recommended"].isin([True, False]) &
           df["received_for_free"].isin([True, False])].copy()

clean["rec"] = clean["recommended"].astype(int)
clean["hrs"] = clean["author.playtime_at_review"] / 60.0
clean["band"] = pd.cut(
    clean["hrs"],
    [-1, 1, 10, 100, 1e9],
    labels=["1_under1h", "2_1to10h", "3_10to100h", "4_over100h"],
)

print("CLEAN RECORDS: %d" % len(clean))

print(clean.groupby("received_for_free")["rec"].agg(["count", "mean"]))

print(clean.groupby(["band", "received_for_free"],
                    observed=True)["rec"].agg(["count", "mean"]))

print("TOTAL: %.1fs" % (time.time() - t0))
