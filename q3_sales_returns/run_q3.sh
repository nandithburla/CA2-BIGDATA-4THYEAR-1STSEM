#!/usr/bin/env bash
# =====================================================================================================
# Q3 - Net-revenue Top-N per category after returns
#   builds the JAR -> uploads orders/returns/products to HDFS -> runs the 3-job pipeline on YARN -> planted-case evidence run
#   -> Top-N-in-mapper counter-example -> cross-checks against an independent reference.
# usage: ./run_q3.sh [--yes] [--pause] [--no-build] [--skip-upload] [--help]
# =====================================================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../scripts/lib.sh"

usage() { sed -n '2,7p' "${BASH_SOURCE[0]}"; }
parse_common_flags "$@"
[[ ${#EXTRA_ARGS[@]} -eq 0 ]] || die "Unknown option: ${EXTRA_ARGS[0]} (try --help)"

MOD="$HERE"; JAR="$MOD/jar/q3_sales_returns.jar"; MAIN="ca2.q3.Q3Driver"
OUT="$MOD/output"; LOG="$MOD/logs"; MET="$OUT/metrics"
H_IN="$HDFS_BASE/q3/input"; H_OUT="$HDFS_BASE/q3/output"
RAW="$DATA_DIR/q3"
R3=$(( REDUCERS < 3 ? 4 : REDUCERS ))      # Job 3 needs >= 3 reducers

banner "Q3  Net revenue Top-5 per category after returns   (HDFS input: $H_IN   output: $H_OUT)"
check_env
build_module q3_sales_returns q3_sales_returns "$MAIN"

step "1. Dataset -> HDFS"
for f in orders returns products; do [[ -f "$RAW/$f.csv" ]] || die "Missing $RAW/$f.csv (set DATA_DIR in config/ca2.env)"; done
if [[ "$SKIP_UPLOAD" == "1" ]]; then hdfs_exists "$H_IN/orders" || die "--skip-upload but $H_IN/orders does not exist"; say "--skip-upload: using existing $H_IN"
else for f in orders returns products; do hdfs_stage_input "$H_IN/$f" "$RAW/$f.csv"; done; fi

step "2. Preparing output locations"
archive_previous "$OUT"; mkdir -p "$OUT" "$LOG" "$MET"
confirm_remove_hdfs "$H_OUT"
hdfs dfs -mkdir -p "$H_OUT"

step "3. Main pipeline: Job 1 (reduce-side join) -> Job 2 (net revenue, replicated join) -> Job 3 (Top-5, custom Partitioner, $R3 reducers)   [single driver]"
run_logged "$LOG/q3_main_pipeline.log" hadoop jar "$JAR" "$MAIN" -Dca2.metrics.dir="$MET/main" \
  --orders "$H_IN/orders/orders.csv" --returns "$H_IN/returns/returns.csv" --products "$H_IN/products/products.csv" \
  --output "$H_OUT/main" --reducers "$R3" --top 5 || die "Pipeline failed - see $LOG/q3_main_pipeline.log"
assert_ran_on_yarn "$LOG/q3_main_pipeline.log"
for d in joined aggregates; do hdfs_exists "$H_OUT/main/$d" && warn "intermediate directory $d still exists" || ok "Driver deleted the intermediate directory $H_OUT/main/$d"; done

hdfs dfs -cat "$H_OUT/main/top5/part-r-*" | LC_ALL=C sort > "$OUT/q3_top5.csv"
{ echo "category,month,rank,product_id,product_name,net_revenue,net_units"; cat "$OUT/q3_top5.csv"; } > "$OUT/q3_top5_with_header.csv"
hdfs dfs -cat "$H_OUT/main/orphan_returns/orphan_returns-r-*" 2>/dev/null | LC_ALL=C sort > "$OUT/q3_orphan_returns.csv" || true
hdfs dfs -ls -h "$H_OUT/main/top5" | tee "$OUT/q3_top5_part_files.txt"
hdfs dfs -ls -h "$H_OUT/main/orphan_returns" | tee "$OUT/q3_orphan_part_files.txt" || true
{ echo "# category,month,rank,product_id,product_name,net_revenue,net_units  (first 20 lines of the sorted output)"; head -20 "$OUT/q3_top5.csv"; } > "$OUT/sample_top5_first20.txt"
{ echo "# return_id,order_id,line_no,qty_returned,return_ts  (first 20 lines)"; head -20 "$OUT/q3_orphan_returns.csv"; } > "$OUT/sample_orphan_returns_first20.txt"

C1="$MET/main/counters_join.tsv"; C2="$MET/main/counters_revenue.tsv"; C3="$MET/main/counters_top.tsv"
step "Counters"
echo "--- Job 1 (join) ---";    print_counters "$C1" "ca2.q3.JoinCounters"    | tee "$OUT/q3_counters_job1_join.txt"
echo "--- Job 2 (revenue) ---"; print_counters "$C2" "ca2.q3.RevenueCounters" | tee "$OUT/q3_counters_job2_revenue.txt"
echo "--- Job 3 (top-N) ---";   print_counters "$C3" "ca2.q3.TopNCounters"    | tee "$OUT/q3_counters_job3_top.txt"
NBUSY=$(counter "$C3" REDUCERS_WITH_OUTPUT)
say "Job 3 reducers: $R3 launched, $NBUSY received at least one category  (assignment: at least 3)  [custom CategoryPartitioner routes by category only]"
[[ "$NBUSY" -ge 3 ]] && ok "at least 3 reducers produced output" || warn "fewer than 3 reducers produced output"
shot "Q3-A" "terminal: the counters above + 'Deleted the intermediate directory' lines   |   RM UI $RM_UI (3 FINISHED Q3 jobs)"
shot "Q3-B" "JobHistory $HISTORY_UI -> job Q3-J1-join-orders-returns -> Counters (RETURN_EXCEEDS_ORDER, RETURNS_ORPHAN, ...);  HDFS listing of top5 part files (output/q3_top5_part_files.txt)"

# ---------------- planted cases: a small EVIDENCE RUN of the same driver on only the planted order ids (O99...) ----------------
step "4. Planted test cases - evidence run of the same driver on the planted orders only (all intermediate files kept so you can see every line)"
EV="$OUT/evidence_input"; mkdir -p "$EV"
awk -F, 'NR==1 || $1 ~ /^O99/' "$RAW/orders.csv"  > "$EV/orders.csv"
awk -F, 'NR==1 || $2 ~ /^O99/' "$RAW/returns.csv" > "$EV/returns.csv"
say "$(($(wc -l < "$EV/orders.csv")-1)) planted order lines, $(($(wc -l < "$EV/returns.csv")-1)) planted returns (local copies of rows of the real files, in $EV)"
H_EV="$HDFS_BASE/q3/evidence"
confirm_remove_hdfs "$H_EV"; hdfs dfs -mkdir -p "$H_EV/in"
hdfs dfs -put "$EV/orders.csv" "$EV/returns.csv" "$H_EV/in/"
run_logged "$LOG/q3_evidence_run.log" hadoop jar "$JAR" "$MAIN" -Dca2.metrics.dir="$MET/evidence" \
  --orders "$H_EV/in/orders.csv" --returns "$H_EV/in/returns.csv" --products "$H_IN/products/products.csv" --output "$H_EV/out" --reducers "$R3" --top 5 --keep-intermediate \
  >/dev/null || die "evidence run failed - see $LOG/q3_evidence_run.log"
hdfs dfs -cat "$H_EV/out/joined/part-r-*" | LC_ALL=C sort > "$OUT/q3_evidence_joined.csv"
hdfs dfs -cat "$H_EV/out/aggregates/part-r-*" | LC_ALL=C sort > "$OUT/q3_evidence_aggregates.csv"
CE="$MET/evidence/counters_join.tsv"
{
  echo "joined line format: order_id,line_no,month,product_id,qty,returned,net_qty,unit_price_cents,net_revenue_cents"
  echo
  echo "== (1) one order line returned in TWO separate returns: O9900001 line 1 (qty 4; returns R9900001 qty 1 + R9900002 qty 2) =="
  grep -h -E '^R99000(01|02),' "$EV/returns.csv"; grep -h '^O9900001,1,' "$OUT/q3_evidence_joined.csv"
  echo "   -> net_qty = 4 - (1+2) = 1, revenue 1 x 2904.00 = 290400 cents.  counter LINES_WITH_MULTIPLE_RETURNS (evidence run) = $(counter "$CE" LINES_WITH_MULTIPLE_RETURNS)"
  echo
  echo "== (2) a return EXCEEDING the ordered quantity: O9900002 line 1 (qty 2, returned 3) =="
  grep -h '^R9900003,' "$EV/returns.csv"; grep -h '^O9900002,1,' "$OUT/q3_evidence_joined.csv"
  echo "   -> returned 3 > qty 2: net_qty capped at 0, revenue 0.  counter RETURN_EXCEEDS_ORDER (evidence run) = $(counter "$CE" RETURN_EXCEEDS_ORDER);  full data set: $(counter "$C1" RETURN_EXCEEDS_ORDER)"
  echo
  echo "== (3) order on 31 January returned on 2 February: O9912034 line 2 =="
  grep -h '^O9912034,2,' "$EV/orders.csv"; grep -h '^R0007731,O9912034,' "$EV/returns.csv"
  grep -h '^O9912034,2,' "$OUT/q3_evidence_joined.csv"
  echo "   -> the revenue is booked in month 2026-01 (the month of the ORDER), not in 2026-02 (month of the return).  Aggregate rows of product P04418 in the evidence run:"
  grep -h ',P04418,' "$OUT/q3_evidence_aggregates.csv"
  echo
  echo "== (4) a product missing from products.csv: P99999 -> category UNMAPPED =="
  grep -h ',P99999,' "$EV/orders.csv"
  grep -h ',P99999,' "$OUT/q3_evidence_aggregates.csv"
  grep -h '^UNMAPPED,' "$OUT/q3_top5.csv"
  echo "   counter LINES_UNMAPPED_PRODUCT (full data) = $(counter "$C2" LINES_UNMAPPED_PRODUCT)"
  echo
  echo "== (5) two products with IDENTICAL revenue and units in the same category-month: P09001 and P09002 (Books, 2026-02) =="
  grep -h -E '^Books,2026-02,P0900[12],' "$OUT/q3_evidence_aggregates.csv"
  grep -h -E '^Books,2026-02,[0-9]+,P0900[12],' "$OUT/q3_top5.csv"
  echo "   -> BOTH survive in the real Top-5 (ranks by product_id asc).  counter TIES_ON_REVENUE (full data) = $(counter "$C3" TIES_ON_REVENUE)"
  echo "      (strict total order: revenue desc, units desc, product_id asc; a TreeMap keyed by revenue alone would drop one of them - see section 5)"
  echo
  echo "== (6) orphan returns (return of a line that is not in orders.csv): MultipleOutputs file orphan_returns-r-*  =="
  echo "   counter RETURNS_ORPHAN (full data) = $(counter "$C1" RETURNS_ORPHAN);  lines in the orphan_returns files: $(wc -l < "$OUT/q3_orphan_returns.csv");  first 3:"
  head -3 "$OUT/q3_orphan_returns.csv"
  echo
  echo "== (7) CANCELLED lines are excluded: counter ORDER_LINES_CANCELLED = $(counter "$C1" ORDER_LINES_CANCELLED) of $(counter "$C1" ORDER_LINES_READ) order lines =="
} 2>&1 | tee "$OUT/q3_planted_cases.txt"
shot "Q3-C" "terminal: the planted-case block above (also saved in output/q3_planted_cases.txt)"
hdfs dfs -rm -r -f -skipTrash "$H_EV" >/dev/null && ok "removed the script's own evidence directory $H_EV"

# ---------------- analysis question ----------------
step "5. Analysis: why can Top-N NOT be applied inside Job 2's mappers?  (counter-example from YOUR data)"
if command -v python3 >/dev/null 2>&1; then
  python3 -I "$CA2_ROOT/tools/q3_topn_counterexample.py" "$RAW" --splits 4 --topn 5 | tee "$OUT/q3_topn_counterexample.txt"
else warn "python3 not found - counter-example skipped"; fi
cat <<'TXT' | tee "$OUT/q3_topn_answer.txt"

Answer: a product's revenue is a SUM over many order lines that are spread over all map tasks of Job 2. A mapper sees only a partial sum for each
product; a product whose partial sum is outside the local Top-5 of every mapper (or of some of them) is dropped before its other partial sums are
added, so the reducer sums an incomplete total and the final Top-5 can contain the wrong products with wrong revenues. Top-N is only valid after the
aggregation is complete (reduce side of Job 2) - which is why Job 3 is a separate job: its mappers only prune rows that are already complete totals.
TXT
shot "Q3-D" "terminal: the counter-example block above"

# ---------------- verification ----------------
step "6. Cross-check with the independent reference (pure Python, raw csv -> expected results)"
VERIF="$OUT/q3_verification.txt"
if command -v python3 >/dev/null 2>&1; then
  mkdir -p "$OUT/reference"
  python3 -I "$CA2_ROOT/tools/reference/q3_reference.py" "$RAW" "$OUT/reference" 5 > "$OUT/reference/summary_stdout.txt" 2>&1 || warn "reference run reported an error"
  RC=0; : > "$VERIF"
  cmp -s "$OUT/reference/top5.csv" "$OUT/q3_top5.csv" && echo "top5            : MATCH ($(wc -l < "$OUT/q3_top5.csv") rows)" >> "$VERIF" || { echo "top5            : MISMATCH" >> "$VERIF"; RC=1; }
  cmp -s "$OUT/reference/orphan_returns.csv" "$OUT/q3_orphan_returns.csv" && echo "orphan_returns  : MATCH ($(wc -l < "$OUT/q3_orphan_returns.csv") rows)" >> "$VERIF" || { echo "orphan_returns  : MISMATCH" >> "$VERIF"; RC=1; }
  chk() { local ref got; ref=$(awk -F'\t' -v c="$1" '$1==c{print $2}' "$OUT/reference/summary.tsv"); got=$(counter "$2" "$1"); [[ "${ref:-x}" == "$got" ]] && echo "counter $1: Hadoop=$got reference=$ref MATCH" >> "$VERIF" || { echo "counter $1: Hadoop=$got reference=${ref:-?} MISMATCH" >> "$VERIF"; RC=1; }; }
  for c in ORDER_LINES_READ RETURN_LINES_READ ORDER_LINES_CANCELLED ORDER_LINES_KEPT LINES_WITH_RETURNS LINES_WITH_MULTIPLE_RETURNS RETURNS_MATCHED RETURNS_ORPHAN RETURN_EXCEEDS_ORDER; do chk $c "$C1"; done
  for c in LINES_UNMAPPED_PRODUCT AGGREGATE_KEYS; do chk $c "$C2"; done
  for c in GROUPS ROWS_WRITTEN TIES_ON_REVENUE; do chk $c "$C3"; done
  cat "$VERIF"
  [[ $RC -eq 0 ]] && ok "VERIFY: Hadoop results equal the independent reference" || warn "VERIFY: differences found - see $VERIF"
else warn "python3 not found - verification skipped (results are NOT verified)"; fi

step "7. Wrap-up"
yarn_apps | tee "$LOG/q3_yarn_applications.txt" || true
grep -h '^@@JOB' "$LOG"/q3_*.log 2>/dev/null | awk -F'\t' 'BEGIN{print "tag\tjob_id\tsuccess\telapsed_ms\ttracking_url"} {print $2"\t"$3"\t"$4"\t"$5"\t"$6}' > "$OUT/q3_jobs_summary.tsv" || true
echo
echo "Viva helper - change N (Top-3) without rebuilding:  hadoop jar $JAR $MAIN --orders $H_IN/orders/orders.csv --returns $H_IN/returns/returns.csv --products $H_IN/products/products.csv --output $H_OUT/viva --top 3"
echo "Results in HDFS : $H_OUT/main/top5  and  $H_OUT/main/orphan_returns"
echo "Local files     : $OUT   (counters in $OUT/metrics, logs in $LOG)"
ok "Q3 finished."
