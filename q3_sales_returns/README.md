# Q3 – Net-revenue Top-5 per category after returns

Package `ca2.q3` · one driver `Q3Driver` (Tool/ToolRunner) · 3 chained jobs · Java 11 · `org.apache.hadoop.mapreduce` only.

## Run
```bash
cd q3_sales_returns
./run_q3.sh            # asks before deleting an existing HDFS output directory
./run_q3.sh --yes --pause
```
Outputs in `output/`: `q3_top5.csv` (final), `q3_orphan_returns.csv`, counters, planted-case evidence, Top-N counter-example, verification. Logs in `logs/`.
Viva: `--top 3` (change N) or `--reducers 5` on the `hadoop jar` command printed at the end of the script.

## Pipeline
| Job | Pattern | Details |
|---|---|---|
| J1 `Q3-J1-join-orders-returns` | **reduce-side join**, `MultipleInputs` (OrderMapper + ReturnMapper), `MultipleOutputs` `orphan_returns` | key `JoinKey(order_id, line_no, tag)` tag 0 = order, 1 = return; Partitioner on (order_id,line_no); grouping on (order_id,line_no) → the order record arrives first, returns are *streamed* (only a running sum is kept); CANCELLED lines excluded; `net_qty = qty − Σ returned`, capped at 0 with counter `RETURN_EXCEEDS_ORDER` |
| J2 `Q3-J2-net-revenue-replicated-join` | **replicated join** (products.csv in the Distributed Cache, loaded in `Mapper.setup()`), Combiner | key `(category, order month, product_id)`, value `AggWritable(units, cents)`; unknown product → `UNMAPPED`; month = month of the **order** |
| J3 `Q3-J3-top5-per-category-month` | **Top-N**, custom `CategoryPartitioner`, ≥ 3 reducers | mappers keep a bounded `TreeSet` (≤ 5 per group, strict total order) and emit in `cleanup()`; reducer merges and writes `category,month,rank,product_id,product_name,net_revenue,net_units` |

* Money is handled in integer cents (exact sums, order independent).
* The orphan returns are a final result: the driver moves `orphan_returns-r-*` out of the intermediate `joined/` directory before it deletes `joined/` and `aggregates/`.
* `return_id` is **not** unique in this dataset (`R0007731` appears twice, once for the planted 31-Jan order) – returns are therefore never de-duplicated by `return_id`.
* Strict total order (`Candidate.BEST_FIRST`): revenue desc, units desc, product_id asc – two products with identical revenue/units (P09001 / P09002) are never merged.
* Counters (enums): `JoinCounters`, `RevenueCounters`, `TopNCounters`. `PRODUCTS_LOADED` is counted once **per task** that loads the cache file (so it is a multiple of the number of tasks).

## Why Top-N cannot be applied in Job 2's mappers
See `output/q3_topn_answer.txt` and the counter-example computed from your own data (`tools/q3_topn_counterexample.py`): with 4 simulated mappers the Top-5 is wrong in
72 of 73 groups; e.g. Automotive/2026-01: product P03440 has partial sums 726 167.55 + 532 522.87 + 822 989.89 + 145 233.51 – the last partial is not in that mapper's local
Top-5, so the reducer sees 2 081 680.31 instead of 2 226 913.82. (The mapper split here is a simulation of 4 equal chunks, not the actual YARN splits.)

## Verified vs not verified
Verified here (Hadoop-API simulator + independent Python reference, real dataset): Top-5 file, orphan returns and every counter equal the reference.
**Not verified here:** real Hadoop 3.3.6 compilation, Maven, YARN execution, JobHistory.
