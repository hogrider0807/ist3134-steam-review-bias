#!/usr/bin/env python3
"""
Hadoop Streaming mapper: Steam review acquisition-channel bias (RQ2).

Emits one key-value pair per valid review record:

    key   = "<received_for_free>|<playtime_band>"
    value = 1 if recommended else 0

Hadoop divides the input at arbitrary byte offsets. Because review bodies
contain embedded newlines, a split can begin part-way through a quoted
field, leaving the CSV reader positioned inside an unterminated string.
Such rows are counted and skipped rather than allowed to raise, which
would fail the map task and, after four attempts, the entire job.

Custom counters report retention directly from the job:
    Steam / KeptRecords
    Steam / SkippedRecords

Usage (local test):
    head -n 5000 steam_reviews.csv | python3 mapper.py | sort | python3 reducer.py
"""

import sys
import csv

# Positional column indices in steam_reviews.csv (see README).
REC = 8       # recommended
FREE = 14     # received_for_free
PLAYTIME = 21  # author.playtime_at_review

BOOL = {"True": 1, "False": 0}

# Default limit is 131,072 characters; long review bodies exceed it.
csv.field_size_limit(10 * 1024 * 1024)


def classify(minutes):
    """Bin playtime in minutes into four ordinal bands."""
    hours = minutes / 60.0
    if hours < 1:
        return "1_under1h"
    if hours < 10:
        return "2_1to10h"
    if hours < 100:
        return "3_10to100h"
    return "4_over100h"


def main():
    reader = csv.reader(sys.stdin)
    kept = 0
    skipped = 0

    while True:
        try:
            row = next(reader)
        except StopIteration:
            break
        except csv.Error:
            # Split boundary landed inside a quoted field.
            skipped += 1
            continue
        except Exception:
            skipped += 1
            continue

        if len(row) < 23:
            skipped += 1
            continue

        rec, free = row[REC], row[FREE]
        if rec not in BOOL or free not in BOOL:
            skipped += 1
            continue

        try:
            minutes = int(float(row[PLAYTIME]))
        except (ValueError, TypeError):
            skipped += 1
            continue

        if minutes < 0:
            skipped += 1
            continue

        kept += 1
        print("%s|%s\t%d" % (free, classify(minutes), BOOL[rec]))

    sys.stderr.write("reporter:counter:Steam,KeptRecords,%d\n" % kept)
    sys.stderr.write("reporter:counter:Steam,SkippedRecords,%d\n" % skipped)


if __name__ == "__main__":
    main()
