#!/usr/bin/env bash
# =====================================================================================================
# Q4 - Clickstream sessionization (secondary sort) and conversion funnel
#   builds the JAR -> uploads the click logs to HDFS -> runs the 2-job pipeline on YARN with mapreduce.reduce.memory.mb=512
#   -> planted-case evidence run -> cross-checks against an independent reference.
# usage: ./run_q4.sh [--yes] [--pause] [--no-build] [--skip-upload] [--help]
#   Q4_REDUCE_MEMORY_MB (default 512) and Q4_REDUCE_HEAP_MB (default 384) in config/ca2.env or the environment.
# =====================================================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../scripts/lib.sh"

usage() { sed -n '2,8p' "${BASH_SOURCE[0]}"; }
parse_common_flags "$@"
[[ ${#EXTRA_ARGS[@]} -eq 0 ]] || die "Unknown option: ${EXTRA_ARGS[0]} (try --help)"

MOD="$HERE"; JAR="$MOD/jar/q4_clickstream.jar"; MAIN="ca2.q4.Q4Driver"
OUT="$MOD/output"; LOG="$MOD/logs"; MET="$OUT/metrics"
H_IN="$HDFS_BASE/q4/input"; H_OUT="$HDFS_BASE/q4/output"
RAW="$DATA_DIR/q4"
MEM_MB="${Q4_REDUCE_MEMORY_MB:-512}"; HEAP_MB="${Q4_REDUCE_HEAP_MB:-384}"
MEMOPTS=(-Dmapreduce.reduce.memory.mb="$MEM_MB" -Dmapreduce.reduce.java.opts="-Xmx${HEAP_MB}m"
         -Dmapreduce.map.memory.mb=512 -Dmapreduce.map.java.opts=-Xmx384m
         -Dyarn.app.mapreduce.am.resource.mb=512 -Dyarn.app.mapreduce.am.command-opts=-Xmx384m)

banner "Q4  Clickstream sessions + funnel   (HDFS input: $H_IN   output: $H_OUT)"
check_env
build_module q4_clickstream q4_clickstream "$MAIN"

step "1. Dataset -> HDFS"
shopt -s nullglob; CLICKS=("$RAW"/clicks/clicks_part*.log); shopt -u nullglob
[[ ${#CLICKS[@]} -gt 0 ]] || die "No click logs in $RAW/clicks (set DATA_DIR in config/ca2.env)"
[[ ${#CLICKS[@]} -eq 6 ]] || warn "expected 6 click log files, found ${#CLICKS[@]}"
if [[ "$SKIP_UPLOAD" == "1" ]]; then hdfs_exists "$H_IN/clicks" || die "--skip-upload but $H_IN/clicks does not exist"; say "--skip-upload: using existing $H_IN"
else hdfs_stage_input "$H_IN/clicks" "${CLICKS[@]}"; fi

step "2. Preparing output locations"
archive_previous "$OUT"; mkdir -p "$OUT" "$LOG" "$MET"
confirm_remove_hdfs "$H_OUT"
hdfs dfs -mkdir -p "$H_OUT"

step "3. Main pipeline with reducer memory capped:  mapreduce.reduce.memory.mb=$MEM_MB  mapreduce.reduce.java.opts=-Xmx${HEAP_MB}m"
run_logged "$LOG/q4_main_pipeline.log" hadoop jar "$JAR" "$MAIN" "${MEMOPTS[@]}" -Dca2.metrics.dir="$MET/main" \
  --input "$H_IN/clicks" --output "$H_OUT/main" --reducers "$REDUCERS" || die "Pipeline failed - see $LOG/q4_main_pipeline.log"
assert_ran_on_yarn "$LOG/q4_main_pipeline.log"
hdfs_exists "$H_OUT/main/sessions/intermediate" && warn "intermediate directory still exists" || ok "Driver deleted the intermediate directory $H_OUT/main/sessions/intermediate"
if grep -q -i -E 'OutOfMemoryError|Container killed|beyond.*memory limit|Killed by external signal' "$LOG/q4_main_pipeline.log"; then warn "memory-related failure text found in the log - check $LOG/q4_main_pipeline.log"; else ok "No OutOfMemoryError / 'Container killed' in the job log"; fi

hdfs dfs -cat "$H_OUT/main/sessions/part-r-*"            | LC_ALL=C sort > "$OUT/q4_sessions.csv"
hdfs dfs -cat "$H_OUT/main/sessions/bots-r-*"            | LC_ALL=C sort > "$OUT/q4_bots.csv"
hdfs dfs -cat "$H_OUT/main/funnel/part-r-*"                              > "$OUT/q4_funnel.csv"
hdfs dfs -cat "$H_OUT/main/sessions/reducer_memory-r-*"                  > "$OUT/q4_reducer_memory.txt"
hdfs dfs -ls -h "$H_OUT/main/sessions" | tee "$OUT/q4_hdfs_sessions_listing.txt" >/dev/null
{ echo "# user_id,session_seq,start_ts,end_ts,duration_sec,page_views,distinct_urls,entry_url,exit_url (first 20 lines of the sorted output)"; head -20 "$OUT/q4_sessions.csv"; } > "$OUT/sample_sessions_first20.txt"
{ echo "# session columns + reason (first 20 lines)"; head -20 "$OUT/q4_bots.csv"; } > "$OUT/sample_bots_first20.txt"
{ echo "# ist_date,sessions,home,product,cart,success,home_to_product_pct,product_to_cart_pct,cart_to_success_pct (first 20 lines)"; head -20 "$OUT/q4_funnel.csv"; } > "$OUT/sample_funnel_first20.txt"

C1="$MET/main/counters_sessions.tsv"; C2="$MET/main/counters_funnel.tsv"
step "Counters"
echo "--- Job 1 (sessions) ---"; print_counters "$C1" "ca2.q4.SessionCounters" | tee "$OUT/q4_counters_job1_sessions.txt"
echo "--- Job 2 (funnel) ---";   print_counters "$C2" "ca2.q4.FunnelCounters"  | tee "$OUT/q4_counters_job2_funnel.txt"
shot "Q4-A" "terminal: counters above + 'Deleted the intermediate directory' + RM UI $RM_UI (2 FINISHED Q4 jobs)"

step "4. Memory proof: the 300 000-event user was streamed by a reducer with mapreduce.reduce.memory.mb=$MEM_MB (heap -Xmx${HEAP_MB}m)"
cat "$OUT/q4_reducer_memory.txt" | tee -a /dev/null
echo "Users with more than 100 000 events: $(counter "$C1" USERS_OVER_100K_EVENTS);  job finished successfully; the largest user is in the line with largest_user_events=300000 above."
echo "(max_heap_mb is the JVM limit really in force in that reducer; peak_used_heap_mb is sampled every 65 536 events and includes garbage that has not been collected yet.)"
echo "YARN rounds a container up to yarn.scheduler.minimum-allocation-mb (default 1024); the JVM heap limit -Xmx${HEAP_MB}m is what makes the cap real."
shot "Q4-B" "JobHistory $HISTORY_UI -> job Q4-J1-sessionize-secondary-sort -> Configuration (search 'mapreduce.reduce.memory.mb' = $MEM_MB and 'mapreduce.reduce.java.opts') + Counters; plus the memory lines above"

# ---------------- planted cases ----------------
step "5. Planted test cases - evidence run of the same driver on the planted users only (U9000001..5 and U0048812)"
EV="$OUT/evidence_input"; mkdir -p "$EV"
grep -h -E '^(U900000[1-5]|U0048812)\|' "$RAW"/clicks/*.log > "$EV/planted_events.log"
say "$(wc -l < "$EV/planted_events.log") planted events copied from the raw files (they come from $(grep -l -E '^(U900000[1-5])\|' "$RAW"/clicks/*.log | wc -l) different files)"
H_EV="$HDFS_BASE/q4/evidence"
confirm_remove_hdfs "$H_EV"; hdfs dfs -mkdir -p "$H_EV/in"; hdfs dfs -put "$EV/planted_events.log" "$H_EV/in/"
run_logged "$LOG/q4_evidence_default.log" hadoop jar "$JAR" "$MAIN" -Dca2.metrics.dir="$MET/evidence_default" --input "$H_EV/in" --output "$H_EV/default" --reducers 3 >/dev/null || die "evidence run failed"
run_logged "$LOG/q4_evidence_botmin3.log"  hadoop jar "$JAR" "$MAIN" -Dq4.bot.min.events=3 -Dca2.metrics.dir="$MET/evidence_botmin3" --input "$H_EV/in" --output "$H_EV/botmin3" --reducers 3 >/dev/null || die "evidence run (bot.min.events=3) failed"
ev_cat() { { hdfs dfs -cat "$H_EV/$1/$2" 2>/dev/null || true; } | LC_ALL=C sort; }
{
  echo "columns: user_id,session_seq,start_ts,end_ts,duration_sec,page_views,distinct_urls,entry_url,exit_url   (IST = +05:30)"
  echo
  echo "== (1) two events exactly 30:00 apart -> SAME session (a new session needs a gap STRICTLY greater than 30 minutes): U9000001 =="
  grep -h '^U9000001|' "$EV/planted_events.log" | sort -t'|' -k2,2n | awk -F'|' '{printf "   raw: %s ts=%s %s\n", $1, $2, $3}'
  ev_cat default 'sessions/part-r-*' | grep '^U9000001,' || true
  echo "   counter GAP_EXACTLY_30_MIN (full data) = $(counter "$C1" GAP_EXACTLY_30_MIN)"
  echo
  echo "== (2) a session that crosses midnight IST: U9000002 (23:50, 23:55 | 00:00, 00:05) -> split by the DATE change although the gaps are 5 minutes =="
  ev_cat default 'sessions/part-r-*' | grep '^U9000002,' || true
  echo "   counter SPLIT_BY_MIDNIGHT_IST (full data) = $(counter "$C1" SPLIT_BY_MIDNIGHT_IST)"
  echo
  echo "== (3) events of one user in REVERSE order across different files: U9000003 (file index of each raw event) =="
  grep -H '^U9000003|' "$RAW"/clicks/clicks_part*.log | awk -F'[|:]' '{n=split($1,p,"/"); printf "   %s ts=%s %s\n", p[n], $3, $4}' || true
  ev_cat default 'sessions/part-r-*' | grep '^U9000003,' || true
  echo "   -> one session, entry /home (earliest), exit /checkout/success (latest) although the file order is reversed: secondary sort on (user_id, event_ts)"
  echo
  echo "== (4) two events with an IDENTICAL timestamp: U9000004 (raw events in two files) =="
  grep -h '^U9000004|' "$EV/planted_events.log" | awk -F'|' '{printf "   raw: %s ts=%s %s\n", $1, $2, $3}'
  echo "   default rule  (bot if average gap < 1 s, any session with >= 2 events):"
  ev_cat default 'sessions/part-r-*' | grep '^U9000004,' | sed 's/^/      sessions: /' || true
  ev_cat default 'sessions/bots-r-*'  | grep '^U9000004,' | sed 's/^/      bots    : /' || true
  echo "   -Dq4.bot.min.events=3 (viva switch: the gap rule only applies to sessions with >= 3 events):"
  ev_cat botmin3 'sessions/part-r-*' | grep '^U9000004,' | sed 's/^/      sessions: /' || true
  ev_cat botmin3 'sessions/bots-r-*'  | grep '^U9000004,' | sed 's/^/      bots    : /' || true
  echo "   The two events are ordered deterministically (timestamp, then url ascending): entry /home, exit /product/P00014.  counter IDENTICAL_TIMESTAMP_EVENTS (full data) = $(counter "$C1" IDENTICAL_TIMESTAMP_EVENTS)"
  echo "   NOTE: the assignment's bot rule taken literally (average inter-event gap below 1 second) is satisfied by a 2-event session with gap 0, so U9000004 is a 'bot' under"
  echo "         the default setting. Pick the setting you want to defend in the viva and say so in the report."
  echo
  echo "== (5) a session that reaches /cart BEFORE /product: U9000005 (events: /home 18:00, /cart 18:02, /product/P00015 18:04) =="
  ev_cat default 'sessions/part-r-*' | grep '^U9000005,' || true
  echo "   funnel row of its date (evidence run: only this user + the other planted users on that date):"
  ev_cat default 'funnel/part-r-*' | grep '^2026-09-16' || true
  echo "   -> stages reached: home=1 product=1 cart=0 success=0: cart came before product, so it is NOT a product->cart conversion (product_to_cart_pct = 0.00)."
  echo
  echo "== (6) the bot user U9999999 (300 000 events) -> bots file, excluded from sessions and funnel =="
  grep -c '^U9999999,' "$OUT/q4_bots.csv" | sed 's/^/   bot sessions of U9999999: /'
  head -2 "$OUT/q4_bots.csv" | grep U9999999 | sed 's/^/   /'
  echo "   in the sessions file: $(grep -c '^U9999999,' "$OUT/q4_sessions.csv") lines (expected 0)"
  echo
  echo "== funnel of all planted dates in the evidence run =="
  ev_cat default 'funnel/part-r-*'
} 2>&1 | tee "$OUT/q4_planted_cases.txt"
shot "Q4-C" "terminal: the planted-case block above (also saved in output/q4_planted_cases.txt)"
hdfs dfs -rm -r -f -skipTrash "$H_EV" >/dev/null && ok "removed the script's own evidence directory $H_EV"

step "6. Verification: bot exclusion and funnel"
echo "bot sessions: $(wc -l < "$OUT/q4_bots.csv"),  normal sessions: $(wc -l < "$OUT/q4_sessions.csv"),  funnel dates: $(wc -l < "$OUT/q4_funnel.csv")"
echo "sum of sessions in the funnel = $(awk -F, '{s+=$2} END{print s}' "$OUT/q4_funnel.csv")  (must equal the number of normal sessions = $(counter "$C1" SESSIONS_NORMAL))"
shot "Q4-D" "terminal: first lines of the funnel (output/sample_funnel_first20.txt) and the session samples"

step "7. Cross-check with the independent reference (pure Python, raw logs -> expected results)"
VERIF="$OUT/q4_verification.txt"
if command -v python3 >/dev/null 2>&1; then
  mkdir -p "$OUT/reference"
  python3 -I "$CA2_ROOT/tools/reference/q4_reference.py" "$RAW/clicks" "$OUT/reference" > "$OUT/reference/summary_stdout.txt" 2>&1 || warn "reference run reported an error"
  RC=0; : > "$VERIF"
  cmp -s "$OUT/reference/sessions.csv" "$OUT/q4_sessions.csv" && echo "sessions : MATCH ($(wc -l < "$OUT/q4_sessions.csv") rows)" >> "$VERIF" || { echo "sessions : MISMATCH" >> "$VERIF"; RC=1; }
  cmp -s "$OUT/reference/bots.csv"     "$OUT/q4_bots.csv"     && echo "bots     : MATCH ($(wc -l < "$OUT/q4_bots.csv") rows)" >> "$VERIF" || { echo "bots     : MISMATCH" >> "$VERIF"; RC=1; }
  cmp -s "$OUT/reference/funnel.csv"   "$OUT/q4_funnel.csv"   && echo "funnel   : MATCH ($(wc -l < "$OUT/q4_funnel.csv") dates)" >> "$VERIF" || { echo "funnel   : MISMATCH" >> "$VERIF"; RC=1; }
  chk() { local ref got; ref=$(awk -F'\t' -v c="$1" '$1==c{print $2}' "$OUT/reference/summary.tsv"); got=$(counter "$C1" "$1"); [[ "${ref:-x}" == "$got" ]] && echo "counter $1: Hadoop=$got reference=$ref MATCH" >> "$VERIF" || { echo "counter $1: Hadoop=$got reference=${ref:-?} MISMATCH" >> "$VERIF"; RC=1; }; }
  for c in LINES_READ EVENTS USERS SESSIONS SESSIONS_NORMAL SESSIONS_BOT BOT_BY_EVENT_COUNT BOT_BY_AVG_GAP SPLIT_BY_GAP SPLIT_BY_MIDNIGHT_IST GAP_EXACTLY_30_MIN IDENTICAL_TIMESTAMP_EVENTS USERS_OVER_100K_EVENTS; do chk $c; done
  cat "$VERIF"
  [[ $RC -eq 0 ]] && ok "VERIFY: Hadoop results equal the independent reference" || warn "VERIFY: differences found - see $VERIF"
else warn "python3 not found - verification skipped (results are NOT verified)"; fi

step "8. Wrap-up"
yarn_apps | tee "$LOG/q4_yarn_applications.txt" || true
grep -h '^@@JOB' "$LOG"/q4_*.log 2>/dev/null | awk -F'\t' 'BEGIN{print "tag\tjob_id\tsuccess\telapsed_ms\ttracking_url"} {print $2"\t"$3"\t"$4"\t"$5"\t"$6}' > "$OUT/q4_jobs_summary.tsv" || true
echo
echo "Viva helpers (no rebuild): change the session gap to 15 minutes:  hadoop jar $JAR $MAIN -Dq4.gap.ms=900000 --input $H_IN/clicks --output $H_OUT/viva"
echo "                           change the bot threshold:               -Dq4.bot.events=100        gap-rule minimum events: -Dq4.bot.min.events=3"
echo "Results in HDFS : $H_OUT/main/sessions (part-r-*, bots-r-*, reducer_memory-r-*) and $H_OUT/main/funnel"
echo "Local files     : $OUT   (counters in $OUT/metrics, logs in $LOG)"
ok "Q4 finished."
