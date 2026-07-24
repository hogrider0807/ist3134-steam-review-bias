#!/usr/bin/env python3
"""
Chi-square test of independence: acquisition channel vs recommendation.

Takes the contingency counts produced by the MapReduce and Spark jobs on
the full corpus and tests, for the aggregate table and within each
playtime stratum, whether recommendation is independent of whether the
reviewer received the game free of charge.

Implemented from first principles rather than via SciPy, which is not
present in the default EMR Python environment. For one degree of freedom
the chi-square survival function has a closed form:

    P(X > x) = erfc(sqrt(x / 2))

Effect size is reported as the phi coefficient, phi = sqrt(chi2 / n).
With a sample of 21.7 million records, significance is attained even for
negligible differences, so phi and the percentage-point difference are the
appropriate basis for interpretation rather than the p-value alone.

Usage:
    python3 chisquare.py
"""

import math


def chi2_2x2(a, b, c, d):
    """Chi-square statistic for a 2x2 table.

    a = free & recommended       b = free & not recommended
    c = paid & recommended       d = paid & not recommended

    Returns (chi2, phi, n).
    """
    n = a + b + c + d
    observed = [[a, b], [c, d]]
    row_totals = [a + b, c + d]
    col_totals = [a + c, b + d]

    chi2 = 0.0
    for i in range(2):
        for j in range(2):
            expected = row_totals[i] * col_totals[j] / n
            chi2 += (observed[i][j] - expected) ** 2 / expected

    return chi2, math.sqrt(chi2 / n), n


def p_value(x):
    """Upper-tail probability of chi-square with 1 degree of freedom."""
    return math.erfc(math.sqrt(x / 2.0))


def report(label, free_rec, free_n, paid_rec, paid_n):
    """Run the test for one stratum and print a formatted result block."""
    a, b = free_rec, free_n - free_rec
    c, d = paid_rec, paid_n - paid_rec

    chi2, phi, n = chi2_2x2(a, b, c, d)
    p = p_value(chi2)
    rate_free, rate_paid = a / free_n, c / paid_n

    print("--- %s ---" % label)
    print("  free    : %9d / %9d = %.4f" % (a, free_n, rate_free))
    print("  paid    : %9d / %9d = %.4f" % (c, paid_n, rate_paid))
    print("  diff    : %+.2f pp" % ((rate_free - rate_paid) * 100))
    print("  chi2    : %.2f  (df = 1, n = %d)" % (chi2, n))
    print("  p       : %s" % ("< 1e-300" if p < 1e-300 else "%.3e" % p))
    print("  phi     : %.5f" % phi)
    print()


# Counts from the full-corpus MapReduce and Spark output.
report("AGGREGATE", 610008, 686832, 18391433, 21034834)

report("under 1h",  int(0.7075 * 49889),  49889,
                    int(0.6608 * 658032),  658032)
report("1-10h",     int(0.8875 * 211014), 211014,
                    int(0.8802 * 4782407), 4782407)
report("10-100h",   int(0.9295 * 280917), 280917,
                    int(0.9069 * 9827394), 9827394)
report("over 100h", int(0.8712 * 145012), 145012,
                    int(0.8383 * 5767001), 5767001)

report("RQ3 steam_purchase",
       int(0.8748 * 4898466),  4898466,
       int(0.8747 * 16823200), 16823200)
