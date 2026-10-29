# Q5 – Three-way payment reconciliation (orders × gateway transactions × bank settlements)

Package `ca2.q5` · one driver `Q5Driver` (Tool/ToolRunner) · 3 chained jobs · Java 11 · `org.apache.hadoop.mapreduce` only.

## Run
```bash
cd q5_payment_reconciliation
./run_q5.sh            # builds the JAR if needed, uploads the 4 files, runs on YARN, planted-case evidence, verification
./run_q5.sh --yes --pause
```
Outputs in `output/`: `categories/<category>.csv` (sorted copy of every category), `q5_summary.csv`, counters, shuffle evidence, `q5_planted_cases.txt`, `q5_design_answers.txt`, `q5_verification.txt`. Logs in `logs/`.

## Pipeline
| Job | Pattern | Details |
|---|---|---|
| J1 `Q5-J1-join-txn-settlement` | reduce-side **full outer join**, tagged key, MultipleInputs, MultipleOutputs | key `TaggedKey(txn_id, tag)`: transaction = tag 0 (arrives first), settlement = tag 1. Streams the settlements into (sum, count). Output: one record per transaction; settlements with no transaction → `orphan_settlement` |
| J2 `Q5-J2-join-order-txn-classify` | reduce-side **full outer join**, tagged key, **Distributed Cache** (`fx_rates.csv`), MultipleOutputs | key `TaggedKey(order_id, tag)`: order = tag 0, transactions = tag 1 (streamed). Converts to INR at the **order-date** rate, applies the 5 rules, writes one directory per category; transactions with no order → `orphan_txn` |
| J3 `Q5-J3-category-summary` | summarization + Combiner | count and INR totals per category → `summary/` |

Both joins: `IdPartitioner` (id only) · natural sort `(id, tag)` · `IdGroupingComparator` (id only). Only running sums and two flags are held in the reducers – the "many" side is never buffered.

Output directories (under the output path): `unpaid underpaid overpaid paid_not_settled settlement_mismatch matched orphan_txn orphan_settlement summary`.
Line format (all categories): `category,id,customer_id,currency,order_amount,order_inr,net_paid_inr,settled_inr,txn_count,ref1,ref2` (INR with 6 decimals; `ref1/ref2` = order id + txn time for ORPHAN_TXN, txn id + settlement date for ORPHAN_SETTLEMENT).
Summary: `category,count,order_amount_inr_total,net_paid_inr_total,settled_inr_total`.

## Rules (in this order)
Net paid = Σ SUCCESS − Σ REFUNDED, every amount converted at the rate of the **order date**.
1. `UNPAID` net paid = 0 · 2. `UNDERPAID` / `OVERPAID` |net paid − order amount (INR)| > ₹1.00 · 3. `PAID_NOT_SETTLED` any SUCCESS txn has no settlement · 4. `SETTLEMENT_MISMATCH` |Σ settled − net paid| > ₹1.00 · 5. `MATCHED`.
Arithmetic is exact: amounts in cents × FX rate in 10⁻⁴ = integer micro-INR (no floating point), so results do not depend on record arrival order.

## Interpretation notes you must know for the viva
1. The order amount is converted with the same order-date rate as the transactions, so a USD order paid in full is not "underpaid" by FX drift.
2. Σ settled counts every settlement of every transaction of the order. A transaction settled twice therefore doubles the settled total → `SETTLEMENT_MISMATCH` (planted case O9000004).
3. `UNPAID` is decided by net paid = 0, so it contains orders without any transaction **and** orders whose only attempts FAILED or were fully refunded.
4. A negative net paid (refund larger than payment) is not 0 → it falls into rule 2 (UNDERPAID).
5. ORPHAN_TXN / ORPHAN_SETTLEMENT are not orders: their INR value is reported in `net_paid_inr` (signed: SUCCESS +, REFUNDED −, FAILED 0, at the txn's own date) and `settled_inr` respectively; `order_amount_inr` is 0.
6. If an FX date is missing the most recent earlier date is used (counter `FX_FALLBACK_PREVIOUS_DAY`; 0 on this data set, which covers 2026-09-01…30).
7. Duplicate `txn_id` / `order_id` records: first one wins, counted in `DUPLICATE_*` (0 on this data set). `settlement_id`s are not de-duplicated.

## Verified vs not verified
Verified here (Hadoop-API simulator + independent Python reference, real dataset): all 8 category files, the summary and every counter listed in `output/q5_verification.txt` equal the reference.
**Not verified here:** compilation against the real Hadoop 3.3.6 jars, the Maven build, YARN execution, JobHistory, and the run script on a real cluster.
