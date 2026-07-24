# Detecting Review Bias in User-Generated Content at Scale

**IST3134 Big Data Analytics in the Cloud — Group Assignment**
Sunway University · BSc (Hons) Information Systems (Data Analytics)

| | |
|---|---|
| **Cheong Jun Yuan** | 22065585 |
| **Tang Yong Zhe** | 22073464 |
| **Lecturer** | Prof Lau Sian Lun |

---

## Research question

> Do reviewers who received a game free of charge exhibit a significantly higher
> recommendation rate than those who purchased it, and does this difference persist
> once player engagement is taken into account?

Free review copies are standard pre-launch marketing on Steam. If recipients
systematically recommend at a higher rate than paying purchasers, the aggregate
recommendation percentage shown on a store page no longer reflects genuine consumer
satisfaction. Unusually, Steam records the acquisition channel as a structured field,
so the bias can be tested directly rather than inferred.

### Sub-questions

| | |
|---|---|
| **RQ1** | Relationship between playtime at review and probability of recommendation |
| **RQ2** | Effect of free acquisition on recommendation rate, controlling for playtime |
| **RQ3** | Whether external key activation predicts different recommendation behaviour |

---

## Dataset

**Steam Reviews 2021** — https://www.kaggle.com/datasets/najzeko/steam-reviews-2021

| Property | Value |
|---|---|
| Records | 21,721,666 valid reviews (48,296,910 physical lines) |
| Attributes | 23 columns |
| Size | 7.7 GB uncompressed CSV |
| Class balance | 87.4% recommended / 12.6% not recommended |

The review text is deliberately **excluded**. Reviews span many languages, and applying
sentiment analysis uniformly across them would confound the acquisition-channel effect
with a language effect. The analysis uses only numerical, boolean and categorical
attributes, which keeps it language-agnostic.

### Columns used

| Index | Column | Role |
|---|---|---|
| 8 | `recommended` | Dependent variable |
| 13 | `steam_purchase` | Independent variable (RQ3) |
| 14 | `received_for_free` | Independent variable (RQ2) |
| 21 | `author.playtime_at_review` | Control variable (RQ1) |

---

## Results

### RQ2 — reject H₀

| Acquisition channel | Reviews | Rate |
|---|---|---|
| Received free of charge | 686,832 | **88.81%** |
| Purchased | 21,034,834 | **87.43%** |
| **Difference** | | **+1.38 pp** |

χ²(1, N = 21,721,666) = 1158.78, p = 5.55 × 10⁻²⁵⁴, φ = 0.0073

The effect persists in **every** playtime stratum, so it is not explained by
differing engagement between the two groups:

| Playtime band | Free | Purchased | Diff | χ² | p |
|---|---|---|---|---|---|
| Under 1 hour | 70.75% | 66.08% | **+4.67 pp** | 453.20 | 1.5e-100 |
| 1–10 hours | 88.75% | 88.02% | +0.73 pp | 102.24 | 4.9e-24 |
| 10–100 hours | 92.95% | 90.69% | +2.26 pp | 1662.03 | < 1e-300 |
| Over 100 hours | 87.12% | 83.83% | +3.29 pp | 1133.91 | 1.4e-248 |

The effect is **largest among reviewers with under an hour of play** — the group with
the least basis for judgement. Where direct experience is thin, the circumstances of
acquisition carry proportionally more weight.

### RQ1 — non-monotonic

| Playtime band | Reviews | Rate |
|---|---|---|
| Under 1 hour | 707,921 | 66.41% |
| 1–10 hours | 4,993,428 | 88.05% |
| 10–100 hours | 10,108,321 | **90.75%** |
| Over 100 hours | 5,912,019 | 83.91% |

Rate peaks in the 10–100 hour band and declines thereafter. Extended exposure appears
to surface defects invisible in shorter sessions.

### RQ3 — fail to reject H₀

87.48% vs 87.47%, χ² = 0.35, p = 0.556. A clean null. External key activation pools
promotional copies with retail keys, bundles and gifts, so it is too noisy a proxy to
detect the effect that `received_for_free` captures directly.

---

## Performance benchmark

| Volume | Size | Records | Hadoop MR | Spark (multiLine) | Spark (split) | pandas |
|---|---|---|---|---|---|---|
| s1m | 167 MB | 460,879 | 34.4 s | 41.8 s | 36.7 s | ParserError |
| s5m | 805 MB | 2,434,968 | 37.0 s | 71.2 s | 40.0 s | **22.2 s** |
| s10m | 1.6 GB | 4,717,435 | 42.8 s | 109.0 s | 45.6 s | 40.6 s |
| **full** | **7.7 GB** | **21,721,666** | 161.3 s | 514.2 s | **89.9 s** | **OOM — killed** |

All jobs on one m5.xlarge primary + three m5.xlarge core nodes (EMR 7.13.0).

### Three findings

**The crossover sits between 1.6 GB and 7.7 GB.** At 805 MB pandas is the *fastest*
of all four — the data fits in memory and the distributed engines pay startup costs
that exceed the work. At 7.7 GB pandas does not complete: the OOM killer terminated it
after 2 min 11 s, with the kernel recording `anon-rss:9016880kB` against 16 GB of node
memory. Below the crossover, distribution is counterproductive; above it, the
single-node approach produces no result at any speed.

**Spark scales sublinearly.** Between 1.6 GB and 7.7 GB — a 4.8× volume increase —
MapReduce grew 3.8× while Spark grew 2.0×. Spark materialises the cleaned dataset once
and reuses it; each MapReduce job re-reads and re-parses from HDFS.

**One option cost 5.7×.** `multiLine=True` prevents Spark from splitting the input.
Instrumenting the script showed **1 partition** with the option enabled against **61**
without — the entire 7.7 GB processed by a single task while three nodes sat idle. The
completeness it bought: 23 extra records out of 21.7 million. Nothing in the API
signals that a boolean argument disables parallelism, and the query returns correct
results either way. Only the timing reveals it.

### Sample size changes the conclusion

| Sample | Records | Difference | Conclusion |
|---|---|---|---|
| s1m | 460,882 | −0.12 pp | No effect / reversed |
| s5m | 2,434,971 | −0.76 pp | Reversed |
| s10m | 4,717,440 | −1.23 pp | Reversed |
| **Full corpus** | **21,721,666** | **+1.38 pp** | **H1 supported** |

Every subset gives a difference in the *opposite* direction. An analyst working with
460,882 records — not a small dataset — would have reached a conclusion wrong in sign.
The subsets are contiguous blocks rather than random samples, which is exactly what
taking the first *n* rows of an oversized file produces. This is the clearest single
justification in the study for processing the complete population.

---

## Repository layout

```
├── mapreduce/
│   ├── mapper.py               # Emits (channel, band) -> recommended
│   └── reducer.py              # Aggregates rate per group
├── spark/
│   ├── steam_bias.py           # DataFrame API + Spark SQL (multiLine=True)
│   └── steam_bias_split.py     # Split-enabled variant (multiLine=False)
├── pandas/
│   └── baseline.py             # Single-node non-big-data baseline
├── analysis/
│   └── chisquare.py            # Chi-square test, no SciPy dependency
├── benchmark/
│   └── benchmark.csv           # Execution times, all implementations
└── results/
    └── mapreduce_full_output.txt
```

---

## Reproducing

### 1. Provision the cluster

Amazon EMR 7.13.0, Hadoop 3.4.2 + Spark 3.5.6, 1 × m5.xlarge primary + 3 × m5.xlarge
core, **100 GiB EBS root volume** (the 15 GiB default is insufficient).

### 2. Stage the data

Download onto the cluster rather than uploading — AWS-to-Kaggle takes seconds.

```bash
pip3 install --user kaggle
export PATH=$PATH:~/.local/bin
mkdir -p ~/.kaggle
echo '{"username":"...","key":"..."}' > ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json

mkdir -p /mnt1/steam && cd /mnt1/steam
kaggle datasets download -d najzeko/steam-reviews-2021
unzip steam-reviews-2021.zip && rm -f steam-reviews-2021.zip

hdfs dfs -mkdir -p /user/hadoop/steam
hdfs dfs -put steam_reviews.csv /user/hadoop/steam/
```

Benchmark subsets:

```bash
head -n 1000001  steam_reviews.csv > s1m.csv
head -n 5000001  steam_reviews.csv > s5m.csv
head -n 10000001 steam_reviews.csv > s10m.csv
```

For the pandas baseline, remove a trailing margin so the file does not end inside a
quoted field:

```bash
head -n 5000001 steam_reviews.csv | head -n -50 > s5m_fix.csv
```

### 3. Run

```bash
# Verify the pipeline locally first
head -n 5000 steam_reviews.csv | python3 mapper.py | sort | python3 reducer.py

# Hadoop MapReduce
time mapred streaming -files mapper.py,reducer.py \
  -input  /user/hadoop/steam/steam_reviews.csv \
  -output /user/hadoop/steam/mr-full \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py"

hdfs dfs -cat /user/hadoop/steam/mr-full/part-* | sort

# Spark
time spark-submit --master yarn --deploy-mode client \
  steam_bias_split.py hdfs:///user/hadoop/steam/steam_reviews.csv 2>/dev/null

# pandas baseline (expect OOM on the full file)
time python3 baseline.py /mnt1/steam/steam_reviews.csv

# Statistics
python3 chisquare.py
```

### 4. Persist before terminating

HDFS is destroyed with the cluster. Copy to S3 first:

```bash
aws s3 cp mapper.py s3://<bucket>/code/
hadoop distcp /user/hadoop/steam/mr-full s3://<bucket>/results/mr-full
```

---

## Problems encountered

| Problem | Cause | Resolution |
|---|---|---|
| Extraction failed, disk full | 15 GiB root volume too small for 3 GB archive + 7.7 GB extract | Rebuilt with 100 GiB root; work directed to `/mnt1` |
| 3 of 4 MapReduce jobs aborted with `PipeMapRed subprocess failed with code 1` | Split boundaries inside quoted review bodies raised an uncaught `csv.Error` | Raised `csv.field_size_limit`; wrapped read loop to skip malformed rows |
| Spark 3.2× slower than MapReduce at full scale | `multiLine=True` disabled input splitting — 1 partition | Benchmarked `multiLine=False` variant: 61 partitions, 89.9 s |
| pandas `ParserError` on every subset | Line truncation cut inside a quoted field | Regenerated subsets with 50-line trailing margin removed |
| pandas terminated on full corpus | 7.7 GB CSV exceeded 16 GB node memory as a DataFrame | Recorded as the expected single-node failure point |

---

## Limitations

**Observational design.** Playtime is controlled by stratification, but unmeasured
differences between populations remain possible. This is an association, not a causal
estimate.

**Self-reported flag.** `received_for_free` is set by the reviewer, not verified.
Under-reporting biases the measured effect towards zero, so +1.38 pp is a conservative
lower bound.

**Effect size.** φ = 0.0073 is weak by conventional standards. The tiny p-values
reflect sample size, not magnitude. The result is statistically unambiguous but
practically modest — and substantially smaller than the ~half-star inflation reported
for Amazon, plausibly because Steam discloses the free-copy flag by default and its
binary format offers less scope for graded inflation than a five-point scale.

**Benchmark caveats.** Single runs, not averaged. Subsets are contiguous blocks, not
random samples. The MapReduce implementation omits a combiner, which would narrow the
gap against Spark.

**Temporal scope.** The corpus reflects the platform as of 2021.

---

## References

Dean, J., & Ghemawat, S. (2008). MapReduce: Simplified data processing on large
clusters. *Communications of the ACM*, 51(1), 107–113.

Zaharia, M., Chowdhury, M., Das, T., Dave, A., Ma, J., McCauley, M., Franklin, M. J.,
Shenker, S., & Stoica, I. (2012). Resilient distributed datasets: A fault-tolerant
abstraction for in-memory cluster computing. *NSDI '12*, 15–28.

Zeko, N. (2021). *Steam Reviews 2021* [Data set]. Kaggle.

Full reference list in the report.
