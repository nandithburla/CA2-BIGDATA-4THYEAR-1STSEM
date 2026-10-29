#!/usr/bin/env bash
# =====================================================================================================
# Q5 - Three-way payment reconciliation (orders x gateway transactions x bank settlements, FX via Distributed Cache)
#   builds the JAR -> uploads the 4 files to HDFS -> runs the 3-job pipeline on YARN -> planted-case evidence run
#   -> join-order / replicated-join justification -> cross-checks against an independent reference.
# usage: ./run_q5.sh [--yes] [--pause] [--no-build] [--skip-upload] [--help]
# =====================================================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../scripts/lib.sh"

usage() { sed -n '2,7p' "${BASH_SOURCE[0]}"; }
parse_common_flags "$@"
[[ ${#EXTRA_ARGS[@]} -eq 0 ]] || die "Unknown option: ${EXTRA_ARGS[0]} (try --help)"

MOD="$HERE"; JAR="$MOD/jar/q5_payment_reconciliation.jar"; MAIN="ca2.q5.Q5Driver"
OUT="$MOD/output"; LOG="$MOD/logs"; MET="$OUT/metrics"
H_IN="$HDFS_BASE/q5/input"; H_OUT="$HDFS_BASE/q5/output"
RAW="$DATA_DIR/q5"
R=$(( REDUCERS < 2 ? 4 : REDUCERS ))

banner "Q5  Payment reconciliation   (HDFS input: $H_IN   output: $H_OUT)"
check_env
build_module q5_payment_reconciliation q5_payment_reconciliation "$MAIN"

step "1. Dataset -> HDFS"
for f in orders gateway_txns bank_settlements fx_rates; do [[ -f "$RAW/$f.csv" ]] || die "Missing $RAW/$f.csv (set DATA_DIR in config/ca2.env)"; done
if [[ "$SKIP_UPLOAD" == "1" ]]; then hdfs_exists "$H_IN/orders" || die "--skip-upload but $H_IN/orders does not exist"; say "--skip-upload: using existing $H_IN"
else for f in orders gateway_txns bank_settlements fx_rates; do hdfs_stage_input "$H_IN/$f" "$RAW/$f.csv"; done; fi

step "2. Preparing output locations"
archive_previous "$OUT"; mkdir -p "$OUT" "$LOG" "$MET"
confirm_remove_hdfs "$H_OUT"
hdfs dfs -mkdir -p "$H_OUT"

ARGS_IN=(--orders "$H_IN/orders/orders.csv" --txns "$H_IN/gateway_txns/gateway_txns.csv" --settlements "$H_IN/bank_settlements/bank_settlements.csv" --fx "$H_IN/fx_rates/fx_rates.csv")

step "3. Main pipeline: Job 1 (txn x settlement on txn_id) -> Job 2 (order x txn on order_id + FX + classify) -> Job 3 (summary)   [single driver, $R reducers]"
run_logged "$LOG/q5_main_pipeline.log" hadoop jar "$JAR" "$MAIN" -Dmapreduce.map.memory.mb=512 -Dmapreduce.map.java.opts=-Xmx384m -Dmapreduce.reduce.memory.mb=512 -Dmapreduce.reduce.java.opts=-Xmx384m -Dyarn.app.mapreduce.am.resource.mb=512 -Dyarn.app.mapreduce.am.command-opts=-Xmx384m -Dca2.metrics.dir="$MET/main" \
  "${ARGS_IN[@]}" --output "$H_OUT/main" --reducers "$R" || die "Pipeline failed - see $LOG/q5_main_pipeline.log"
assert_ran_on_yarn "$LOG/q5_main_pipeline.log"
hdfs_exists "$H_OUT/main/_work" && warn "intermediate directory _work still exists" || ok "Driver deleted the intermediate directory $H_OUT/main/_work"

CATS="unpaid underpaid overpaid paid_not_settled settlement_mismatch matched orphan_txn orphan_settlement"
mkdir -p "$OUT/categories"
for c in $CATS; do
  if hdfs_exists "$H_OUT/main/$c"; then hdfs dfs -cat "$H_OUT/main/$c/*-r-*" | LC_ALL=C sort > "$OUT/categories/$c.csv"
  else : > "$OUT/categories/$c.csv"; fi
done
hdfs dfs -cat "$H_OUT/main/summary/part-r-*" > "$OUT/q5_summary.csv"
hdfs dfs -ls -R "$H_OUT/main" | tee "$OUT/q5_output_tree.txt" >/dev/null
{ echo "category,id,customer_id,currency,order_amount,order_inr,net_paid_inr,settled_inr,txn_count,ref1,ref2"; } > "$OUT/categories/_columns.txt"
for c in $CATS; do head -5 "$OUT/categories/$c.csv" > "$OUT/categories/sample_$c.txt"; done

step "Summary file (count and INR total per category)"
column -s, -t "$OUT/q5_summary.csv" 2>/dev/null | tee "$OUT/q5_summary_pretty.txt" || cat "$OUT/q5_summary.csv"
step "Category directories in HDFS (one MultipleOutputs directory per category)"
hdfs dfs -ls "$H_OUT/main" | tee "$OUT/q5_category_dirs.txt"
for c in $CATS; do printf '  %-20s %9s lines\n' "$c" "$(wc -l < "$OUT/categories/$c.csv")"; done | tee "$OUT/q5_category_line_counts.txt"

C1="$MET/main/counters_txn_settlement.tsv"; C2="$MET/main/counters_reconcile.tsv"; C3="$MET/main/counters_summary.tsv"
step "Counters"
echo "--- Job 1 (txn x settlement) ---"; print_counters "$C1" "ca2.q5.TxnJoinCounters" | tee "$OUT/q5_counters_job1.txt"
echo "--- Job 2 (order x txn, classify) ---"; print_counters "$C2" "ca2.q5.ReconCounters" | tee "$OUT/q5_counters_job2.txt"
echo "--- Job 3 (summary) ---"; print_counters "$C3" "ca2.q5.SummaryCounters" | tee "$OUT/q5_counters_job3.txt"
{
  echo "shuffle evidence (framework counters)             Job1(txn x settlement)   Job2(order x txn)"
  for n in MAP_OUTPUT_RECORDS REDUCE_SHUFFLE_BYTES REDUCE_INPUT_GROUPS REDUCE_INPUT_RECORDS; do printf '  %-40s %12s %18s\n' "$n" "$(counter "$C1" $n)" "$(counter "$C2" $n)"; done
} | tee "$OUT/q5_shuffle_evidence.txt"
shot "Q5-A" "terminal: summary table + category directory listing + counters above   |   RM UI $RM_UI (3 FINISHED Q5 jobs)"
shot "Q5-B" "JobHistory $HISTORY_UI -> job Q5-J1-join-txn-settlement -> Counters (ORPHAN_SETTLEMENTS, TXNS_SETTLED_MULTIPLE_TIMES) and Q5-J2 -> Counters (CAT_*)"

# ---------------- planted cases ----------------
step "4. Planted test cases - evidence run of the same driver on the planted rows only (intermediate files kept)"
EV="$OUT/evidence_input"; mkdir -p "$EV"
awk -F, 'NR==1 || $1 ~ /^O9/' "$RAW/orders.csv" > "$EV/orders.csv"
awk -F, 'NR==1 || $2 ~ /^O9/' "$RAW/gateway_txns.csv" > "$EV/gateway_txns.csv"
awk -F, 'NR==1 || $2 ~ /^(T0020642[3-9]|T9999999[0-9])$/' "$RAW/bank_settlements.csv" > "$EV/bank_settlements.csv"
say "$(($(wc -l < "$EV/orders.csv")-1)) planted orders, $(($(wc -l < "$EV/gateway_txns.csv")-1)) planted transactions, $(($(wc -l < "$EV/bank_settlements.csv")-1)) planted settlements (local copies of rows of the real files, in $EV)"
H_EV="$HDFS_BASE/q5/evidence"
confirm_remove_hdfs "$H_EV"; hdfs dfs -mkdir -p "$H_EV/in"
hdfs dfs -put "$EV/orders.csv" "$EV/gateway_txns.csv" "$EV/bank_settlements.csv" "$H_EV/in/"
run_logged "$LOG/q5_evidence_run.log" hadoop jar "$JAR" "$MAIN" -Dmapreduce.map.memory.mb=512 -Dmapreduce.map.java.opts=-Xmx384m -Dmapreduce.reduce.memory.mb=512 -Dmapreduce.reduce.java.opts=-Xmx384m -Dyarn.app.mapreduce.am.resource.mb=512 -Dyarn.app.mapreduce.am.command-opts=-Xmx384m -Dca2.metrics.dir="$MET/evidence" \
  --orders "$H_EV/in/orders.csv" --txns "$H_EV/in/gateway_txns.csv" --settlements "$H_EV/in/bank_settlements.csv" --fx "$H_IN/fx_rates/fx_rates.csv" \
  --output "$H_EV/out" --reducers "$R" --keep-intermediate >/dev/null || die "evidence run failed - see $LOG/q5_evidence_run.log"
EVC="$OUT/evidence_categories"; mkdir -p "$EVC"
for c in $CATS; do hdfs_exists "$H_EV/out/$c" && hdfs dfs -cat "$H_EV/out/$c/*-r-*" | LC_ALL=C sort > "$EVC/$c.csv" || : > "$EVC/$c.csv"; done
hdfs dfs -cat "$H_EV/out/_work/joined_txn/part-r-*" | LC_ALL=C sort > "$OUT/q5_evidence_joined_txn.csv"
{
  echo "category line: category,id,customer_id,currency,order_amount,order_inr,net_paid_inr,settled_inr,txn_count,ref1,ref2   (amounts in INR with 6 decimals = exact micro-INR)"
  echo "joined_txn line (Job 1 -> Job 2): order_id,txn_id,amount_cents,currency,status,txn_ts,settled_micro_inr,settlement_count"
  echo
  echo "== (1) FAILED then SUCCESS: O9000001 (T00206423 FAILED 10:01, T00206424 SUCCESS 10:03, settled once) =="
  grep -h '^O9000001,' "$OUT/q5_evidence_joined_txn.csv"; grep -h ',O9000001,' "$EVC"/*.csv
  echo "   -> the FAILED attempt adds nothing to net paid; only the SUCCESS counts.  Order amount 1500.00 = net paid 1500.00 = settled 1500.00 -> MATCHED"
  echo
  echo "== (2) SUCCESS followed by a partial REFUND: O9000002 (2000.00 paid, 500.00 refunded, 1500.00 settled) =="
  grep -h '^O9000002,' "$OUT/q5_evidence_joined_txn.csv"; grep -h ',O9000002,' "$EVC"/*.csv
  echo "   -> net paid = 2000 - 500 = 1500; |1500 - 2000| > 1.00 -> UNDERPAID (rule 2 fires before the settlement rules)"
  echo
  echo "== (3) USD order needing FX: O9000003 (120.00 USD on 2026-09-10) =="
  grep -h '^2026-09-10,USD,' "$RAW/fx_rates.csv"; grep -h '^O9000003,' "$OUT/q5_evidence_joined_txn.csv"; grep -h ',O9000003,' "$EVC"/*.csv
  echo "   -> 120.00 x rate of the ORDER date = order_inr = net_paid_inr = settled 9990.90 -> MATCHED;  FX_NON_INR_CONVERSIONS (evidence run) = $(counter "$MET/evidence/counters_reconcile.tsv" FX_NON_INR_CONVERSIONS)"
  echo
  echo "== (4) a transaction settled twice: T00206428 / O9000004 (750.00, settlements S00186830 + S00186831 of 750.00 each) =="
  grep -h ',T00206428,' "$OUT/q5_evidence_joined_txn.csv"; grep -h ',O9000004,' "$EVC"/*.csv
  echo "   -> settled 1500.00 vs net paid 750.00 -> SETTLEMENT_MISMATCH.  counter TXNS_SETTLED_MULTIPLE_TIMES: evidence run = $(counter "$MET/evidence/counters_txn_settlement.tsv" TXNS_SETTLED_MULTIPLE_TIMES), full data = $(counter "$C1" TXNS_SETTLED_MULTIPLE_TIMES)"
  echo
  echo "== (5) a transaction for an unknown order: T00206429 -> O9999999 =="
  grep -h 'T00206429' "$EVC/orphan_txn.csv"
  echo "   -> ORPHAN_TXN (its own category directory).  counter CAT_ORPHAN_TXN: evidence run = $(counter "$MET/evidence/counters_reconcile.tsv" CAT_ORPHAN_TXN), full data = $(counter "$C2" CAT_ORPHAN_TXN)"
  echo
  echo "== (6) a settlement for an unknown transaction: S00186832 -> T99999999 =="
  grep -h 'S00186832' "$EVC/orphan_settlement.csv"
  echo "   -> ORPHAN_SETTLEMENT.  counter ORPHAN_SETTLEMENTS: evidence run = $(counter "$MET/evidence/counters_txn_settlement.tsv" ORPHAN_SETTLEMENTS), full data = $(counter "$C1" ORPHAN_SETTLEMENTS)"
  echo
  echo "== (7) an order with no transaction at all: O9000005 =="
  grep -h ',O9000005,' "$EVC"/*.csv
  echo "   -> net paid 0 -> UNPAID.  counter ORDERS_WITHOUT_TXN: evidence run = $(counter "$MET/evidence/counters_reconcile.tsv" ORDERS_WITHOUT_TXN), full data = $(counter "$C2" ORDERS_WITHOUT_TXN)"
} 2>&1 | tee "$OUT/q5_planted_cases.txt"
shot "Q5-C" "terminal: the planted-case block above (also saved in output/q5_planted_cases.txt)"
hdfs dfs -rm -r -f -skipTrash "$H_EV" >/dev/null && ok "removed the script's own evidence directory $H_EV"

# ---------------- design justification ----------------
step "5. Design justification (the two written answers of the question)"
NORD=$(wc -c < "$RAW/orders.csv")
cat <<TXT | tee "$OUT/q5_design_answers.txt"
JOIN ORDER - why txn x settlement FIRST (key txn_id), order x transaction SECOND (key order_id):
  * The question joins on two different keys, so one shuffle can never serve both; two chained jobs (one shuffle each) is the minimum.
  * Job 1 collapses the many side: every settlement of a transaction is reduced to ONE number pair (sum, count). Job 2 therefore shuffles one
    record per transaction ($(counter "$C1" TXNS_JOINED) here) instead of transactions + settlements side by side, and its streaming reducer only
    needs running sums.
  * The reverse order (orders x txn on order_id first) would have to re-key every transaction by txn_id for the settlement join and then re-key by
    order_id again to classify - an extra shuffle of the largest table.
  * Net paid needs the ORDER date (FX) and the settled total needs the settlements of ALL the order's transactions, so only after both joins is
    every input of the classification rules available - Job 2 is the natural place for it.
  * Both reducers use a tagged composite key (id, tag): partitioner = id, sort = (id, tag), grouping = id. The "one" side (transaction / order)
    arrives first; the many side (settlements / transactions) is STREAMED into running sums - nothing is buffered.

WHY A REPLICATED (MAP-SIDE) JOIN IS UNSUITABLE FOR orders:
  * orders.csv is the large, growing side ($NORD bytes, $(counter "$C2" ORDER_LINES_READ) rows here): a replicated join must load the whole table into every
    mapper's heap as a HashMap of Java objects (typically several times the file size), which does not scale and is the opposite of the "small table" case.
  * It cannot produce a FULL OUTER join: orders that have no transaction (UNPAID, $(counter "$C2" ORDERS_WITHOUT_TXN) here) never appear in the transaction
    stream, so a map-side join can not output them - that needs a global group-by, i.e. a reduce.
  * fx_rates.csv (90 rows) IS small, so that table is the one shipped through the Distributed Cache.
TXT

# ---------------- verification ----------------
step "6. Cross-check with the independent reference (pure Python, raw csv -> expected results)"
VERIF="$OUT/q5_verification.txt"
if command -v python3 >/dev/null 2>&1; then
  mkdir -p "$OUT/reference"
  python3 -I "$CA2_ROOT/tools/reference/q5_reference.py" "$RAW" "$OUT/reference" > "$OUT/reference/summary_stdout.txt" 2>&1 || warn "reference run reported an error"
  RC=0; : > "$VERIF"
  for c in $CATS; do
    cmp -s "$OUT/reference/$c.csv" "$OUT/categories/$c.csv" && echo "$c : MATCH ($(wc -l < "$OUT/categories/$c.csv") rows)" >> "$VERIF" || { echo "$c : MISMATCH" >> "$VERIF"; RC=1; }
  done
  tail -n +2 "$OUT/q5_summary.csv" | LC_ALL=C sort > "$OUT/.sum_got"; tail -n +2 "$OUT/reference/summary.csv" | LC_ALL=C sort > "$OUT/.sum_ref"
  cmp -s "$OUT/.sum_got" "$OUT/.sum_ref" && echo "summary : MATCH" >> "$VERIF" || { echo "summary : MISMATCH" >> "$VERIF"; RC=1; }
  rm -f "$OUT/.sum_got" "$OUT/.sum_ref"
  chk() { local ref got; ref=$(awk -F'\t' -v c="$1" '$1==c{print $2}' "$OUT/reference/summary.tsv"); got=$(counter "$2" "$1"); [[ "${ref:-x}" == "$got" ]] && echo "counter $1: Hadoop=$got reference=$ref MATCH" >> "$VERIF" || { echo "counter $1: Hadoop=$got reference=${ref:-?} MISMATCH" >> "$VERIF"; RC=1; }; }
  for c in TXN_LINES_READ SETTLEMENT_LINES_READ TXNS_JOINED TXNS_WITH_SETTLEMENT TXNS_WITHOUT_SETTLEMENT TXNS_SETTLED_MULTIPLE_TIMES SETTLEMENTS_MATCHED ORPHAN_SETTLEMENTS; do chk $c "$C1"; done
  for c in ORDER_LINES_READ ORDERS ORDERS_WITHOUT_TXN ORDERS_WITH_FAILED_TXN TXNS_SUCCESS TXNS_FAILED TXNS_REFUNDED FX_NON_INR_CONVERSIONS CAT_UNPAID CAT_UNDERPAID CAT_OVERPAID CAT_PAID_NOT_SETTLED CAT_SETTLEMENT_MISMATCH CAT_MATCHED CAT_ORPHAN_TXN; do chk $c "$C2"; done
  cat "$VERIF"
  [[ $RC -eq 0 ]] && ok "VERIFY: Hadoop results equal the independent reference" || warn "VERIFY: differences found - see $VERIF"
else warn "python3 not found - verification skipped (results are NOT verified)"; fi

step "7. Wrap-up"
yarn_apps | tee "$LOG/q5_yarn_applications.txt" || true
grep -h '^@@JOB' "$LOG"/q5_*.log 2>/dev/null | awk -F'\t' 'BEGIN{print "tag\tjob_id\tsuccess\telapsed_ms\ttracking_url"} {print $2"\t"$3"\t"$4"\t"$5"\t"$6}' > "$OUT/q5_jobs_summary.tsv" || true
echo "Results in HDFS : $H_OUT/main/{$(echo $CATS | tr ' ' ',')}  and  $H_OUT/main/summary"
echo "Local files     : $OUT   (counters in $OUT/metrics, logs in $LOG)"
ok "Q5 finished."
