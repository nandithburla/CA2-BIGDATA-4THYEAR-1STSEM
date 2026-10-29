# CSD401 CA-2 – Big Data Tools and Techniques 2 – Questions 1–5

Java 11 · Hadoop 3.3.6 · Maven · `org.apache.hadoop.mapreduce` API only · every question = **one driver class (Tool/ToolRunner)**, jobs chained inside it, run on **YARN**.

| Folder | Question | Main patterns |
|---|---|---|
| `q1_smart_meter` | Smart-meter energy analytics | single-pass statistics, combiner (correct vs deliberately wrong), custom counters |
| `q2_inverted_index` | Positional inverted index, TF-IDF, phrase search | delta-encoded postings, tf-idf, Distributed Cache stop-words, MultipleOutputs, 3 jobs |
| `q3_sales_returns` | Net-revenue Top-N after returns | reduce-side join, replicated join, Top-N, custom Partitioner |
| `q4_clickstream` | Sessionization + funnel | secondary sort, streaming reducer, 512 MB reducer proof, bots |
| `q5_payment_reconciliation` | Three-way reconciliation | 2 chained reduce-side outer joins, tagged keys, Distributed Cache FX, MultipleOutputs |

Each folder has `src/ jar/ output/ logs/ screenshots/ report/`, a `run_qN.sh` and its own `README.md` (design, interpretation notes, how to explain it in the viva).

## 1. Install (does not touch your existing `~/ca2_project`)
```bash
cd ~
unzip ca2_bigdata_project.zip            # creates ~/ca2_bigdata_project
cd ca2_bigdata_project
chmod +x */run_q*.sh scripts/*.sh        # only if unzip lost the execute bits
cp config/ca2.env.example config/ca2.env # optional; defaults: DATA_DIR=$HOME/ca2_data/CA2_Dataset, HDFS_BASE=/user/$USER/ca2
```
Start HDFS and YARN + JobHistory first (`start-dfs.sh; start-yarn.sh; mapred --daemon start historyserver`; `jps` must show NameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer).

## 2. Run (one question at a time)
```bash
cd ~/ca2_bigdata_project/q1_smart_meter && ./run_q1.sh        # same for q2 … q5
```
Useful flags: `--pause` (waits at each screenshot point), `--yes` (auto-confirm), `--no-build`, `--skip-upload`.
Each script: builds the JAR into `jar/`, uploads the input to `/user/$USER/ca2/qN/input`, runs the jobs on YARN, saves logs in `logs/` and results in `output/`, runs the planted test cases, and compares with an independent reference.
**Safety:** input data is never deleted. If an HDFS output directory already exists the script **asks** before removing it; old local `output/` is moved to `output/_previous/<time>/`, never overwritten.
If a job id starts with `job_local` the script stops: set `mapreduce.framework.name=yarn`.

## 3. Screenshots (you take them – nothing is pre-made)
When a script prints `>>> SCREENSHOT Qn-X  <what>` take it then (use `--pause` to stop there) and save it to `qN_*/screenshots/`. Typical set per question:
terminal with the exact command + counters · ResourceManager UI `http://localhost:8088` showing the FINISHED application(s) · JobHistory `http://localhost:19888` counters/configuration page · `hdfs dfs -ls` of the output · the planted-case block.
Per-question lists: `qN_*/report/screenshot_checklist.md`.

## 4. Report
`qN_*/report/report_template.md` follows the question's (i)–(vii) sections; fill in your name/roll number, paste the diagrams/counters/tables from `output/`, add screenshots.

## 5. Read before the viva
`docs/VALIDATION.md` (what was and was not verified) and the "Interpretation notes" in each module README. `validation/` is a **simulator used for testing only** – ignore it, it is not Hadoop.
