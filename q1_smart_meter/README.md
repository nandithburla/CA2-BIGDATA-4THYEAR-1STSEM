# Q1 – Smart-meter energy analytics (single-pass statistics + the Combiner trap)

Package `ca2.q1` · one driver `Q1Driver` (Tool/ToolRunner) · 3 chained jobs · Java 11 · `org.apache.hadoop.mapreduce` only.

## Run
```bash
cd q1_smart_meter
./run_q1.sh            # asks before deleting an existing HDFS output directory
./run_q1.sh --yes      # answer "yes" to those questions (never touches the input data)
./run_q1.sh --help
```
Everything the script prints is also saved: `logs/` (job logs, build log, YARN application list), `output/` (results, counters, diff, comparison tables,
planted-case evidence, verification), `output/metrics/*/counters_*.tsv` (every counter of every job, read from the finished job object).
Lines starting with `>>> SCREENSHOT Qx-y` tell you exactly what to capture.

## Pipeline
| Job | Pattern | Map → Reduce key/value |
|---|---|---|
| J1 `Q1-J1-clean-dedup` | filtering + de-duplication (secondary sort) | `MeterReadingKey(meter,ts)`+kWh text → one clean line per (meter,ts) |
| J2 `Q1-J2-stats_*` | numerical summarization + **Combiner** | `MeterMonthKey(city,meter,month)` → `StatsWritable(count,sum,sumSq,min,max)` |
| J3 `Q1-J3-outliers-replicated-join` | **replicated join** (J2 output in the Distributed Cache) | `MeterMonthKey` → `OutlierWritable(readings, above mean+2σ)` |

* Dirty records: each rejection reason has its own counter (`REJ_BAD_TIMESTAMP`, `REJ_EMPTY_KWH`, `REJ_NEGATIVE_KWH`, …, enum `CleanCounters`); duplicates
  are counted (`DUP_*`). Cross-file duplicates reach different mappers and are removed in the reducer by a composite key (meter, timestamp) with a
  grouping comparator.
* Statistics are exact and order independent (kWh are converted to fixed-point milli-kWh), σ is clamped at 0 when floating-point error makes the variance
  slightly negative. Month = first 7 characters of the (validated) timestamp, so 23:45 and 00:00 across a month end go to different months.
* `--combiner on|off|wrong`: the **wrong** combiner emits per-mapper averages; `run_q1.sh` forces ≥3 splits with
  `-Dmapreduce.input.fileinputformat.split.maxsize` (`Q1_SPLIT_MAXSIZE`, default 4 000 000) and explains the result with `tools/q1_wrong_combiner_explain.py`.
* The driver deletes its intermediate (cleaned) directory and exits non-zero when any stage fails.

## Interpretation note you should know for the viva (planted duplicate)
The "duplicate pair in two files" `M00017 / 2026-03-14 18:15:00` exists as **Pune,1.732 twice** (files 0 and 2) **and** as **Mumbai,1.176** (file 1).
The rule "same meter_id + reading_ts → keep one" is applied to the conflicting group with a configurable policy: `-Dq1.dup.policy=majority`
(default: keep the value that occurs most often → Pune,1.732) or `smallest` (keep the lexicographically smallest line → Mumbai,1.176). Say which one you used.

## Verified vs not verified
Verified here (simulator of the Hadoop API + independent Python reference, on the real dataset): all counters and both result files equal the reference;
with/without Combiner are byte-identical; the wrong-combiner experiment produces 8 wrong meter-month means with 12 splits.
**Not verified here:** compilation against the real Hadoop 3.3.6 jars, the Maven build, execution on YARN, JobHistory numbers. Your run produces those.
