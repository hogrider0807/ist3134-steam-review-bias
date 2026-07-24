#!/usr/bin/env python3
"""
Hadoop Streaming reducer: aggregate recommendation rate per group.

Reads the sorted mapper output and emits one line per distinct key:

    received_for_free \t playtime_band \t positive \t total \t rate

Hadoop guarantees that all values sharing a key arrive contiguously, so a
single pass with a running counter is sufficient. The reducer detects a
key change, emits the completed group, and resets its accumulators.

With eight distinct keys the output is eight lines regardless of input
size.
"""

import sys


def emit(key, positive, total):
    """Write one aggregated group, guarding against division by zero."""
    if total:
        free, band = key.split("|")
        print("%s\t%s\t%d\t%d\t%.4f" % (free, band, positive, total, positive / total))


def main():
    current = None
    total = 0
    positive = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            key, value = line.split("\t", 1)
        except ValueError:
            continue

        if current is not None and key != current:
            emit(current, positive, total)
            total = 0
            positive = 0

        current = key
        total += 1
        positive += int(value)

    if current is not None:
        emit(current, positive, total)


if __name__ == "__main__":
    main()
