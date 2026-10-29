#!/usr/bin/env bash
# =====================================================================================================
# Q1 - Smart-meter energy analytics (single-pass statistics + the combiner trap)
#   builds the JAR -> uploads the readings to HDFS -> runs the 3-job pipeline on YARN -> runs the combiner
#   experiments -> collects counters / samples / evidence -> cross-checks against an independent reference.
# usage: ./run_q1.sh [--yes] [--pause] [--no-build] [--skip-upload] [--skip-experiments] [--help]
# =====================================================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../scripts/lib.sh"

usage() { sed -n '2,8p' "${BASH_SOURCE[0]}"; echo "Config: config/ca2.env (see config/ca2.env.example). Q1_SPLIT_MAXSIZE (default 4000000) forces >=3 splits in experiment (d)."; }
SKIP_EXPERIMENTS=0
parse_common_flags "$@"
for a in "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"; do
  case "$a" in --skip-experiments) SKIP_EXPERIMENTS=1 ;; *) die "Unknown option: $a (try --help)" ;; esac
done

MOD="$HERE"; JAR="$MOD/jar/q1_smart_meter.jar"; MAIN="ca2.q1.Q1Driver"
OUT="$MOD/output"; LOG="$MOD/logs"; MET="$OUT/metrics"
H_IN="$HDFS_BASE/q1/input"; H_OUT="$HDFS_BASE/q1/output"
SPLIT_MAXSIZE="${Q1_SPLIT_MAXSIZE:-4000000}"
RAW_DIR="$DATA_DIR/q1/readings"

banner "Q1  Smart-meter analytics   (HDFS input: $H_IN   output: $H_OUT)"
check_env
build_module q1_smart_meter q1_smart_meter "$MAIN"

# ---- 1. input -------------------------------------------------------------------------------------------
step "1. Dataset -> HDFS"
shopt -s nullglob; RAW_FILES=("$RAW_DIR"/readings_part*.csv); shopt -u nullglob
[[ ${#RAW_FILES[@]} -eq 4 ]] || warn "Expected 4 readings_part*.csv files in $RAW_DIR, found ${#RAW_FILES[@]}"
if [[ "$SKIP_UPLOAD" == "1" ]]; then hdfs_exists "$H_IN" || die "--skip-upload but $H_IN does not exist"; say "--skip-upload: using existing $H_IN"
else hdfs_stage_input "$H_IN" "${RAW_FILES[@]}"; fi
hdfs dfs -ls -h "$H_IN" | tee "$LOG/q1_hdfs_input_listing.txt" >/dev/null || true

# ---- 2. output dirs (ask before removing anything that exists) -------------------------------------------------
step "2. Preparing output locations"
archive_previous "$OUT"
mkdir -p "$OUT" "$LOG" "$MET"
confirm_remove_hdfs "$H_OUT"
hdfs dfs -mkdir -p "$H_OUT"

# ---- 3. main pipeline (Job 1 clean/dedup -> Job 2 stats+combiner -> Job 3 outliers) ------------------------------------
step "3. Main pipeline: clean/dedup -> statistics (Combiner) -> outliers (replicated join)   [single driver, ToolRunner]"
say "Reducers: $REDUCERS"
run_logged "$LOG/q1_main_pipeline.log" hadoop jar "$JAR" "$MAIN" -Dca2.metrics.dir="$MET/main" \
  --input "$H_IN" --output "$H_OUT/main" --stage all --combiner on --reducers "$REDUCERS" \
  || die "Main pipeline failed - see $LOG/q1_main_pipeline.log"
assert_ran_on_yarn "$LOG/q1_main_pipeline.log"
hdfs_exists "$H_OUT/main/clean" && warn "intermediate directory still exists" || ok "Driver deleted its intermediate directory ($H_OUT/main/clean) - as required"

hdfs dfs -cat "$H_OUT/main/stats/part-r-*"    > "$OUT/q1_stats_all.csv"
hdfs dfs -cat "$H_OUT/main/outliers/part-r-*" > "$OUT/q1_outliers_all.csv"
sort -o "$OUT/q1_stats_all.csv" "$OUT/q1_stats_all.csv"; sort -o "$OUT/q1_outliers_all.csv" "$OUT/q1_outliers_all.csv"
sample_file "$OUT/q1_stats_all.csv"    "$OUT/sample_stats_first20.txt"
sample_file "$OUT/q1_outliers_all.csv" "$OUT/sample_outliers_first20.txt"
{ echo "# city,meter_id,month,count,min,max,mean,stddev   (first 20 lines)"; cat "$OUT/sample_stats_first20.txt"; } > "$OUT/sample_stats_first20.txt.tmp" && mv "$OUT/sample_stats_first20.txt.tmp" "$OUT/sample_stats_first20.txt"
{ echo "# city,meter_id,month,readings,above_mean_plus_2sigma,percentage   (first 20 lines)"; head -20 "$OUT/q1_outliers_all.csv"; } > "$OUT/sample_outliers_first20.txt"

C_CLEAN="$MET/main/counters_clean.tsv"
step "Counters - Job 1 (validation / duplicates)"
print_counters "$C_CLEAN" "ca2.q1.CleanCounters" | tee "$OUT/q1_counters_job1_clean.txt"
step "Counters - Job 2 (statistics) and Job 3 (outliers)"
print_counters "$MET/main/counters_stats_combiner.tsv" "ca2.q1" | tee "$OUT/q1_counters_job2_stats.txt"
print_counters "$MET/main/counters_outliers.tsv" "ca2.q1" | tee "$OUT/q1_counters_job3_outliers.txt"
shot "Q1-A" "terminal: the 'Counters' blocks above + 'Deleted intermediate directory' line   |   RM UI $RM_UI (Applications -> the 3 FINISHED Q1 jobs)"
shot "Q1-B" "JobHistory $HISTORY_UI -> job Q1-J1-clean-dedup -> Counters   (shows the REJ_* and DUP_* custom counters)"

# ---- 4. planted test cases --------------------------------------------------------------------------------------
step "4. Evidence for the planted test cases"
PL="$OUT/q1_planted_cases.txt"; : > "$PL"
{
  echo "== (1) a meter with exactly ONE reading in a month (sigma = 0): rows with count = 1 =="
  awk -F, '$4==1' "$OUT/q1_stats_all.csv"
  echo
  echo "== (2) readings at 23:45 and 00:00 across a month boundary: meter M00001 =="
  echo "   (raw readings of M00001 at 2026-03-31 23:45:00 and 2026-04-01 00:00:00 in the input)"
  grep -h -E '^M00001,[^,]*,(2026-03-31 23:45:00|2026-04-01 00:00:00),' "$RAW_DIR"/*.csv || true
  echo "   -> the 23:45 reading is counted in month 2026-03, the 00:00 reading in 2026-04 (month = first 7 chars of reading_ts):"
  grep -E ',M00001,2026-0[34],|^[^,]*,M00001,2026-0[34],' "$OUT/q1_stats_all.csv" || true
  echo
  echo "== (3) duplicate pair in two different input files (two different mappers): M00017 at 2026-03-14 18:15:00 =="
  grep -n -H '^M00017,.*2026-03-14 18:15:00' "$RAW_DIR"/*.csv || true
  echo "   Job 1 counters:  DUP_GROUPS_CROSS_FILE=$(counter "$C_CLEAN" DUP_GROUPS_CROSS_FILE)   DUP_GROUPS_CONFLICTING=$(counter "$C_CLEAN" DUP_GROUPS_CONFLICTING)   DUP_RECORDS_DROPPED=$(counter "$C_CLEAN" DUP_RECORDS_DROPPED)"
  echo "   NOTE: in this dataset the SAME (meter, timestamp) also exists as Mumbai,1.176 (a genuine reading) next to the two identical"
  echo "         Pune,1.732 retransmissions. Default policy q1.dup.policy=majority keeps the retransmitted record (Pune,1.732) -> M00017 gets a"
  echo "         1-reading Pune group. Re-run with  -Dq1.dup.policy=smallest  to keep Mumbai,1.176 instead (viva: change the tie-break rule)."
  echo
  echo "== (4) dirty-record counters (every rejection category has its own counter) =="
  print_counters "$C_CLEAN" "ca2.q1.CleanCounters"
} | tee "$PL"
shot "Q1-C" "terminal: the planted-case evidence block above (also saved in output/q1_planted_cases.txt)"

# ---- 5. experiments (c) and (d) --------------------------------------------------------------------------------------
if [[ "$SKIP_EXPERIMENTS" == "1" ]]; then
  warn "--skip-experiments: parts (c) and (d) NOT run"
else
  step "5. Experiment (c): statistics WITH vs WITHOUT the Combiner (same clean data)"
  run_logged "$LOG/q1_exp_clean.log" hadoop jar "$JAR" "$MAIN" -Dca2.metrics.dir="$MET/exp_clean" \
    --input "$H_IN" --stage clean --clean-dir "$H_OUT/work/clean" --reducers "$REDUCERS" || die "clean stage failed"
  run_logged "$LOG/q1_exp_combiner.log" hadoop jar "$JAR" "$MAIN" -Dca2.metrics.dir="$MET/exp_combiner" \
    --stage stats --combiner on --clean-dir "$H_OUT/work/clean" --stats-dir "$H_OUT/exp/stats_combiner" || die "combiner run failed"
  run_logged "$LOG/q1_exp_nocombiner.log" hadoop jar "$JAR" "$MAIN" -Dca2.metrics.dir="$MET/exp_nocombiner" \
    --stage stats --combiner off --clean-dir "$H_OUT/work/clean" --stats-dir "$H_OUT/exp/stats_nocombiner" || die "no-combiner run failed"
  hdfs dfs -cat "$H_OUT/exp/stats_combiner/part-r-*"   > "$OUT/q1_stats_with_combiner.csv"
  hdfs dfs -cat "$H_OUT/exp/stats_nocombiner/part-r-*" > "$OUT/q1_stats_without_combiner.csv"
  echo "\$ diff output_with_combiner output_without_combiner"                    | tee    "$OUT/q1_combiner_diff.txt"
  if diff "$OUT/q1_stats_with_combiner.csv" "$OUT/q1_stats_without_combiner.csv" >> "$OUT/q1_combiner_diff.txt" 2>&1 && cmp -s "$OUT/q1_stats_with_combiner.csv" "$OUT/q1_stats_without_combiner.csv"; then
    echo "(no differences)  -> BYTE-IDENTICAL: md5 $(md5sum < "$OUT/q1_stats_with_combiner.csv" | cut -d' ' -f1) == $(md5sum < "$OUT/q1_stats_without_combiner.csv" | cut -d' ' -f1)" | tee -a "$OUT/q1_combiner_diff.txt"
    COMB_IDENT="IDENTICAL"
  else
    echo "OUTPUTS DIFFER - see above" | tee -a "$OUT/q1_combiner_diff.txt"; COMB_IDENT="DIFFERENT"
  fi
  CW="$MET/exp_combiner/counters_stats_combiner.tsv"; CN="$MET/exp_nocombiner/counters_stats_nocombiner.tsv"
  {
    printf 'metric\twith_combiner\twithout_combiner\n'
    for m in MAP_INPUT_RECORDS MAP_OUTPUT_RECORDS COMBINE_INPUT_RECORDS COMBINE_OUTPUT_RECORDS MAP_OUTPUT_BYTES MAP_OUTPUT_MATERIALIZED_BYTES REDUCE_SHUFFLE_BYTES SPILLED_RECORDS REDUCE_INPUT_RECORDS REDUCE_INPUT_GROUPS; do
      printf '%s\t%s\t%s\n' "$m" "$(counter "$CW" "$m")" "$(counter "$CN" "$m")"
    done
    printf 'job_elapsed_ms\t%s\t%s\n' "$(grep -h '^@@JOB' "$LOG/q1_exp_combiner.log" | tail -1 | cut -f5)" "$(grep -h '^@@JOB' "$LOG/q1_exp_nocombiner.log" | tail -1 | cut -f5)"
  } | tee "$OUT/q1_combiner_comparison.tsv"
  echo "Result of the diff: $COMB_IDENT"
  shot "Q1-D" "terminal: the diff result + the with/without Combiner table above   |   JobHistory counters of the two Q1-J2-stats_* jobs (Combine input/output records, Map output bytes, Reduce shuffle bytes)"

  step "6. Experiment (d): deliberately WRONG combiner (per-mapper averages) with >= 3 forced input splits"
  say "-Dmapreduce.input.fileinputformat.split.maxsize=$SPLIT_MAXSIZE"
  run_logged "$LOG/q1_exp_wrong.log" hadoop jar "$JAR" "$MAIN" -Dca2.metrics.dir="$MET/exp_wrong" -Dmapreduce.input.fileinputformat.split.maxsize="$SPLIT_MAXSIZE" \
    --stage stats --combiner wrong --clean-dir "$H_OUT/work/clean" --stats-dir "$H_OUT/exp/stats_wrong" || die "wrong-combiner run failed"
  run_logged "$LOG/q1_exp_correct_splits.log" hadoop jar "$JAR" "$MAIN" -Dca2.metrics.dir="$MET/exp_correct_splits" -Dmapreduce.input.fileinputformat.split.maxsize="$SPLIT_MAXSIZE" \
    --stage stats --combiner on --clean-dir "$H_OUT/work/clean" --stats-dir "$H_OUT/exp/stats_correct_splits" || die "correct run with splits failed"
  MAPS=$(awk -F'\t' '$3=="TOTAL_LAUNCHED_MAPS"{print $4}' "$MET/exp_wrong/counters_stats_wrongcombiner.tsv" | head -1)
  say "Map tasks (= input splits) launched by the wrong-combiner job: ${MAPS:-unknown}   (assignment: at least 3)"
  [[ "${MAPS:-0}" -ge 3 ]] || warn "fewer than 3 splits - lower Q1_SPLIT_MAXSIZE in config/ca2.env"
  hdfs dfs -cat "$H_OUT/exp/stats_wrong/part-r-*"          | sort > "$OUT/q1_stats_wrong_combiner.csv"
  hdfs dfs -cat "$H_OUT/exp/stats_correct_splits/part-r-*" | sort > "$OUT/q1_stats_correct_same_splits.csv"
  awk -F, 'NR==FNR {k=$1","$2","$3; cm[k]=$7; cn[k]=$4; next} {k=$1","$2","$3; if (k in cm && cm[k]!=$7) printf "%s  correct: n=%s mean=%s   wrong: n=%s mean=%s\n", k, cn[k], cm[k], $4, $7}' \
      "$OUT/q1_stats_correct_same_splits.csv" "$OUT/q1_stats_wrong_combiner.csv" > "$OUT/q1_wrong_combiner_mean_differences.txt"
  echo "meter-months whose mean became wrong: $(wc -l < "$OUT/q1_wrong_combiner_mean_differences.txt")"
  head -10 "$OUT/q1_wrong_combiner_mean_differences.txt"
  if command -v python3 >/dev/null 2>&1; then
    TMPC="$(mktemp -d)"; hdfs dfs -get "$H_OUT/work/clean/part-r-*" "$TMPC"/
    python3 -I "$CA2_ROOT/tools/q1_wrong_combiner_explain.py" --maxsize "$SPLIT_MAXSIZE" --clean "$TMPC"/part-r-* \
      --correct "$OUT/q1_stats_correct_same_splits.csv" --wrong "$OUT/q1_stats_wrong_combiner.csv" | tee "$OUT/q1_wrong_combiner_explanation.txt"
    rm -rf "$TMPC"
  else
    warn "python3 not found - the numeric explanation step was skipped (the differences file above still shows the wrong means)"
  fi
  shot "Q1-E" "terminal: 'meter-months whose mean became wrong' + the worked example (correct mean vs mean of per-mapper averages)   |   RM UI/JobHistory: number of map tasks of the wrong-combiner job"
fi

# ---- 7. cross-check against the independent reference ---------------------------------------------------------------
step "7. Cross-check with the independent reference implementation (pure Python, raw CSV -> expected results)"
VERIF="$OUT/q1_verification.txt"
if command -v python3 >/dev/null 2>&1 && [[ -d "$RAW_DIR" ]]; then
  mkdir -p "$OUT/reference"
  python3 -I "$CA2_ROOT/tools/reference/q1_reference.py" "$RAW_DIR" "$OUT/reference" > "$OUT/reference/summary_stdout.txt" 2>&1 || warn "reference run reported an error - see $OUT/reference/summary_stdout.txt"
  RC=0
  sort "$OUT/reference/stats.csv"     | cmp -s - "$OUT/q1_stats_all.csv"    && echo "stats    : MATCH ($(wc -l < "$OUT/q1_stats_all.csv") rows)" >  "$VERIF" || { echo "stats    : MISMATCH" > "$VERIF"; RC=1; }
  sort "$OUT/reference/outliers.csv"  | cmp -s - "$OUT/q1_outliers_all.csv" && echo "outliers : MATCH ($(wc -l < "$OUT/q1_outliers_all.csv") rows)" >> "$VERIF" || { echo "outliers : MISMATCH" >> "$VERIF"; RC=1; }
  for c in REJ_BAD_TIMESTAMP REJ_EMPTY_KWH REJ_NEGATIVE_KWH VALID_RECORDS DUP_RECORDS_DROPPED DUP_GROUPS_CROSS_FILE DUP_GROUPS_CONFLICTING CLEAN_RECORDS; do
    ref=$(awk -F'\t' -v c="$c" '$1==c{print $2}' "$OUT/reference/summary.tsv"); got=$(counter "$C_CLEAN" "$c")
    if [[ "${ref:-0}" == "$got" ]]; then echo "counter $c: Hadoop=$got reference=${ref:-0} MATCH" >> "$VERIF"; else echo "counter $c: Hadoop=$got reference=${ref:-0} MISMATCH" >> "$VERIF"; RC=1; fi
  done
  cat "$VERIF"
  [[ $RC -eq 0 ]] && ok "VERIFY: Hadoop results equal the independent reference" || warn "VERIFY: differences found - see $VERIF (check dup policy: reference default is 'majority')"
else
  warn "python3 or $RAW_DIR not available - verification skipped (results are NOT verified)"
fi

# ---- 8. wrap up --------------------------------------------------------------------------------------------------------
step "8. Wrap-up"
yarn_apps | tee "$LOG/q1_yarn_applications.txt" || true
grep -h '^@@JOB' "$LOG"/q1_*.log 2>/dev/null | awk -F'\t' 'BEGIN{print "tag\tjob_id\tsuccess\telapsed_ms\ttracking_url"} {print $2"\t"$3"\t"$4"\t"$5"\t"$6}' > "$OUT/q1_jobs_summary.tsv" || true
if hdfs_exists "$H_OUT/work"; then hdfs dfs -rm -r -f -skipTrash "$H_OUT/work" >/dev/null && ok "removed this script's own intermediate directory $H_OUT/work"; fi
echo
echo "Results kept in HDFS : $H_OUT/main/{stats,outliers}  and  $H_OUT/exp/*"
echo "Local files          : $OUT   (counters in $OUT/metrics, logs in $LOG)"
echo "Open $RM_UI and $HISTORY_UI for the ResourceManager / JobHistory screenshots (see report/screenshot_checklist.md)."
ok "Q1 finished."
