#!/usr/bin/env bash
# =====================================================================================================
# Q2 - Positional inverted index, TF-IDF and phrase search
#   builds the JAR -> uploads corpus + stopwords.txt to HDFS -> runs Job1 (count N) -> Job2 (index) -> Job3 (phrase) on YARN
#   -> runs the delta-encoding experiment -> evidence for the planted cases -> cross-checks against an independent reference.
# usage: ./run_q2.sh [--yes] [--pause] [--no-build] [--skip-upload] [--skip-experiments] [--help]
#   phrase: Q2_PHRASE in config/ca2.env or the environment (default "big data analytics"; 2-3 words)
# =====================================================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../scripts/lib.sh"

usage() { sed -n '2,7p' "${BASH_SOURCE[0]}"; }
SKIP_EXPERIMENTS=0
parse_common_flags "$@"
for a in "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"; do
  case "$a" in --skip-experiments) SKIP_EXPERIMENTS=1 ;; *) die "Unknown option: $a (try --help)" ;; esac
done

MOD="$HERE"; JAR="$MOD/jar/q2_inverted_index.jar"; MAIN="ca2.q2.Q2Driver"
OUT="$MOD/output"; LOG="$MOD/logs"; MET="$OUT/metrics"
H_IN="$HDFS_BASE/q2/input"; H_OUT="$HDFS_BASE/q2/output"
PHRASE="${Q2_PHRASE:-big data analytics}"
RAW_DIR="$DATA_DIR/q2"

banner "Q2  Inverted index + TF-IDF + phrase search   (HDFS input: $H_IN   output: $H_OUT)"
check_env
build_module q2_inverted_index q2_inverted_index "$MAIN"

step "1. Dataset -> HDFS"
shopt -s nullglob; CORPUS=("$RAW_DIR"/corpus/corpus_part*.tsv); shopt -u nullglob
[[ ${#CORPUS[@]} -gt 0 ]] || die "No corpus files in $RAW_DIR/corpus (set DATA_DIR in config/ca2.env)"
[[ -f "$RAW_DIR/stopwords.txt" ]] || die "Missing $RAW_DIR/stopwords.txt"
if [[ "$SKIP_UPLOAD" == "1" ]]; then hdfs_exists "$H_IN/corpus" || die "--skip-upload but $H_IN/corpus does not exist"; say "--skip-upload: using existing $H_IN"
else
  hdfs_stage_input "$H_IN/corpus" "${CORPUS[@]}"
  hdfs_stage_input "$H_IN/stopwords" "$RAW_DIR/stopwords.txt"
fi

step "2. Preparing output locations"
archive_previous "$OUT"; mkdir -p "$OUT" "$LOG" "$MET"
confirm_remove_hdfs "$H_OUT"
hdfs dfs -mkdir -p "$H_OUT"

step "3. Main pipeline: Job 1 (count N) -> Job 2 (index, delta-encoded postings) -> Job 3 (phrase \"$PHRASE\")   [single driver]"
run_logged "$LOG/q2_main_pipeline.log" hadoop jar "$JAR" "$MAIN" -Dphrase="$PHRASE" -Dca2.metrics.dir="$MET/main" \
  --input "$H_IN/corpus" --stopwords "$H_IN/stopwords/stopwords.txt" --output "$H_OUT/main" --stage all --delta on --reducers "$REDUCERS" \
  || die "Main pipeline failed - see $LOG/q2_main_pipeline.log"
assert_ran_on_yarn "$LOG/q2_main_pipeline.log"
hdfs_exists "$H_OUT/main/docs_tmp" && warn "intermediate directory still exists" || ok "Driver deleted its intermediate directory ($H_OUT/main/docs_tmp)"

# collect the index locally (sorted by term; C locale = byte order = Hadoop's Text order)
hdfs dfs -cat "$H_OUT/main/index/part-r-*"         | LC_ALL=C sort > "$OUT/q2_index_main.tsv"
hdfs dfs -cat "$H_OUT/main/index/corpus_stop-r-*"  | LC_ALL=C sort > "$OUT/q2_corpus_stop.tsv"
hdfs dfs -cat "$H_OUT/main/index/rare-r-*"         | LC_ALL=C sort > "$OUT/q2_rare.tsv"
hdfs dfs -cat "$H_OUT/main/phrase/part-r-*"        > "$OUT/q2_phrase_results.tsv"
hdfs dfs -ls "$H_OUT/main/index" | tee "$OUT/q2_hdfs_index_listing.txt" >/dev/null
for f in index_main corpus_stop rare; do
  { head -20 "$OUT/q2_$f.tsv"; } > "$OUT/sample_${f}_first20_full.txt"
  cut -c1-230 "$OUT/sample_${f}_first20_full.txt" | awk '{ if (length($0)==230) print $0" ..."; else print $0 }' > "$OUT/sample_${f}_first20.txt"
done
head -20 "$OUT/q2_phrase_results.tsv" > "$OUT/sample_phrase_first20.txt"

C1="$MET/main/counters_count.tsv"; C2="$MET/main/counters_index_deltaon.tsv"; C3="$MET/main/counters_phrase.tsv"
N=$(counter "$C1" DOCS)
step "Counters"
echo "--- Job 1 (documents / malformed lines) ---";  print_counters "$C1" "ca2.q2.DocCounters"   | tee "$OUT/q2_counters_job1.txt"
echo "--- Job 2 mapper (filtering) ---";             print_counters "$C2" "ca2.q2.TokenCounters" | tee "$OUT/q2_counters_job2_tokens.txt"
echo "--- Job 2 reducer (index) ---";                print_counters "$C2" "ca2.q2.IndexCounters" | tee "$OUT/q2_counters_job2_index.txt"
echo "--- Job 3 (phrase) ---";                       print_counters "$C3" "ca2.q2.PhraseCounters"| tee "$OUT/q2_counters_job3_phrase.txt"
echo "N = $N documents (Job 1 counter DOCS); corpus-stop threshold df > 0.4*N = $(awk -v n="$N" 'BEGIN{printf "%.1f", 0.4*n}')"
echo "corpus_stop terms (df): $(cut -f1,2 "$OUT/q2_corpus_stop.tsv" | tr '\t' '=' | tr '\n' ' ')"
shot "Q2-A" "terminal: counters above (N, TOKENS_*, TERMS_*) + RM UI $RM_UI (3 FINISHED Q2 jobs: Q2-J1-count-docs, Q2-J2-index-delta-on, Q2-J3-phrase)"
shot "Q2-B" "JobHistory $HISTORY_UI -> job Q2-J1-count-docs -> Counters (DocCounters.DOCS = N)"

step "4. Evidence for the planted test cases"
{
  echo "== (1) document D900001 contains \"big data big data analytics\" (+ a second \"big data analytics\") =="
  grep -h '^D900001	' "$RAW_DIR"/corpus/*.tsv
  for w in big data analytics; do printf '%-10s ' "$w"; awk -F'\t' -v w="$w" '$1==w {n=split($3,a," "); for(i=1;i<=n;i++) if (a[i] ~ /^D900001:/) print a[i]}' "$OUT/q2_index_main.tsv"; done
  echo "   phrase \"$PHRASE\" ranking line for D900001 (doc_id, number of phrase occurrences):"; grep -P '^D900001\t' "$OUT/q2_phrase_results.tsv" || echo "   (not in the result)"
  echo
  echo "== (2) a term that occurs only in a title: 'zorbulax' (df = 1, tf = 3 = title boost) -> goes to the rare file, not to the main index =="
  grep -P '^D900002\t' "$RAW_DIR"/corpus/*.tsv | cut -c1-200 | sed 's#.*/##'
  grep -P '^zorbulax\t' "$OUT/q2_rare.tsv" || echo "   zorbulax not found in rare"
  echo "   in main index? $(grep -c -P '^zorbulax\t' "$OUT/q2_index_main.tsv") (expected 0)"
  echo
  echo "== (3) a document with an EMPTY BODY: D900003 (kept; counter EMPTY_BODY_DOCS=$(counter "$C1" EMPTY_BODY_DOCS); its title tokens are indexed) =="
  grep -P '^D900003\t' "$RAW_DIR"/corpus/*.tsv | sed 's#.*/##' | cat -A | cut -c1-120
  grep -h -o -P '(^[a-z0-9]+)(?=\t[0-9]+\t.*D900003:)|D900003:[0-9]+:[0-9.]+:\[[0-9,]*\]' "$OUT/q2_index_main.tsv" "$OUT/q2_rare.tsv" | paste -sd' ' | cut -c1-300
  echo "   (N includes it: N = $N)"
  echo
  echo "== (4) a document where every phrase word exists but never adjacently: D900004 =="
  grep -P '^D900004\t' "$RAW_DIR"/corpus/*.tsv | sed 's#.*/##'
  for w in $PHRASE; do printf '%-10s ' "$w"; awk -F'\t' -v w="$w" '$1==w {n=split($3,a," "); for(i=1;i<=n;i++) if (a[i] ~ /^D900004:/) print a[i]}' "$OUT/q2_index_main.tsv"; done
  echo "   in the phrase result? $(grep -c -P '^D900004\t' "$OUT/q2_phrase_results.tsv") (expected 0)   counter DOCS_WORDS_NOT_ADJACENT = $(counter "$C3" DOCS_WORDS_NOT_ADJACENT)"
} 2>&1 | tee "$OUT/q2_planted_cases.txt"
shot "Q2-C" "terminal: the planted-case block above (also saved in output/q2_planted_cases.txt)"

if [[ "$SKIP_EXPERIMENTS" == "1" ]]; then
  warn "--skip-experiments: delta-encoding comparison NOT run"
else
  step "5. Experiment: shuffle bytes WITH vs WITHOUT delta (gap) encoding of the positions"
  for mode in off fixed; do
    run_logged "$LOG/q2_exp_delta_$mode.log" hadoop jar "$JAR" "$MAIN" -Dca2.metrics.dir="$MET/delta_$mode" \
      --input "$H_IN/corpus" --stopwords "$H_IN/stopwords/stopwords.txt" --output "$H_OUT/delta_$mode" --stage index --delta "$mode" --reducers "$REDUCERS" \
      || die "delta=$mode run failed"
    hdfs dfs -cat "$H_OUT/delta_$mode/index/part-r-*" | LC_ALL=C sort | cmp -s - "$OUT/q2_index_main.tsv" && echo "index with --delta $mode is IDENTICAL to the delta-encoded index" || warn "index with --delta $mode DIFFERS"
  done
  CD="$C2"; CO="$MET/delta_off/counters_index_deltaoff.tsv"; CF="$MET/delta_fixed/counters_index_deltafixed.tsv"
  {
    printf 'metric\tdelta_VInt_gaps\tabsolute_VInt\tabsolute_fixed_int32\n'
    for m in MAP_OUTPUT_RECORDS MAP_OUTPUT_BYTES MAP_OUTPUT_MATERIALIZED_BYTES REDUCE_SHUFFLE_BYTES; do
      printf '%s\t%s\t%s\t%s\n' "$m" "$(counter "$CD" "$m")" "$(counter "$CO" "$m")" "$(counter "$CF" "$m")"
    done
  } | tee "$OUT/q2_delta_comparison.tsv"
  MAXPOS=$(grep -h -o '\[[0-9,]*\]' "$OUT/q2_index_main.tsv" "$OUT/q2_corpus_stop.tsv" "$OUT/q2_rare.tsv" | tr -d '[]' | tr ',' '\n' | sort -n | tail -1)
  {
    echo "Largest token position in this dataset: ${MAXPOS:-?}.  A Hadoop VInt needs 1 byte for 0..127, 2 bytes for 128..255 and 3 bytes up to 65535."
    if [[ "${MAXPOS:-999}" -lt 128 ]]; then
      echo "=> every position of THIS corpus fits in one byte whether it is a gap or an absolute value, so delta encoding cannot save shuffle bytes here (the two VInt columns are equal)."
      echo "   The gain over naive fixed 4-byte ints is the third column. Gap encoding only pays off for long documents - see the demonstration below."
    fi
  } | tee -a "$OUT/q2_delta_comparison.tsv"
  echo; echo "--- ca2.q2.DeltaDemo (no cluster needed): [3,10,25] -> stored [3,7,15] and sizes of the three encodings ---"
  hadoop jar "$JAR" ca2.q2.DeltaDemo 2>&1 | grep -v 'Picked up' | tee "$OUT/q2_delta_demo.txt"
  shot "Q2-D" "terminal: the delta / absolute / fixed table + DeltaDemo output above   |   JobHistory counters of the Q2-J2-index-delta-* jobs (Map output bytes, Reduce shuffle bytes)"
fi

step "6. Is a Combiner applicable in Job 2?   NO"
cat <<'TXT' | tee "$OUT/q2_combiner_answer.txt"
A Combiner is not applicable to Job 2:
 1. Every (term, doc_id) pair is produced exactly ONCE (the mapper already aggregates tf and positions per document), so a combiner would find
    no two values to merge - Combine input records would equal Combine output records and nothing is saved.
 2. The reducer needs the number of postings (df) of the WHOLE corpus before it can compute any tf-idf; df and the global sort
    (tfidf desc, doc_id asc) are not associative/commutative aggregations of partial results, so they cannot be moved into a combiner.
 3. A combiner must have identical input and output types (PostingWritable -> PostingWritable); concatenating postings into one bigger value would
    only add bytes and break the "one value = one posting" contract used to count df.
TXT

step "7. Cross-check with the independent reference (pure Python, raw corpus -> expected index)"
VERIF="$OUT/q2_verification.txt"
if command -v python3 >/dev/null 2>&1; then
  mkdir -p "$OUT/reference"
  python3 -I "$CA2_ROOT/tools/reference/q2_reference.py" "$RAW_DIR/corpus" "$RAW_DIR/stopwords.txt" "$OUT/reference" "$PHRASE" > "$OUT/reference/summary_stdout.txt" 2>&1 || warn "reference run reported an error - see $OUT/reference/summary_stdout.txt"
  RC=0; : > "$VERIF"
  cmp -s "$OUT/reference/index_main.tsv"  "$OUT/q2_index_main.tsv"      && echo "main index  : MATCH ($(wc -l < "$OUT/q2_index_main.tsv") terms)"  >> "$VERIF" || { echo "main index  : MISMATCH" >> "$VERIF"; RC=1; }
  cmp -s "$OUT/reference/corpus_stop.tsv" "$OUT/q2_corpus_stop.tsv"     && echo "corpus_stop : MATCH ($(wc -l < "$OUT/q2_corpus_stop.tsv") terms)" >> "$VERIF" || { echo "corpus_stop : MISMATCH" >> "$VERIF"; RC=1; }
  cmp -s "$OUT/reference/rare.tsv"        "$OUT/q2_rare.tsv"            && echo "rare        : MATCH ($(wc -l < "$OUT/q2_rare.tsv") terms)"        >> "$VERIF" || { echo "rare        : MISMATCH" >> "$VERIF"; RC=1; }
  cmp -s "$OUT/reference/phrase.tsv"      "$OUT/q2_phrase_results.tsv"  && echo "phrase      : MATCH ($(wc -l < "$OUT/q2_phrase_results.tsv") documents)" >> "$VERIF" || { echo "phrase      : MISMATCH" >> "$VERIF"; RC=1; }
  chk() { local ref got; ref=$(awk -F'\t' -v c="$1" '$1==c{print $2}' "$OUT/reference/summary.tsv"); got=$(counter "$2" "$1"); [[ "${ref:-x}" == "$got" ]] && echo "counter $1: Hadoop=$got reference=$ref MATCH" >> "$VERIF" || { echo "counter $1: Hadoop=$got reference=${ref:-?} MISMATCH" >> "$VERIF"; RC=1; }; }
  for c in DOCS MALFORMED_LINES; do chk $c "$C1"; done
  for c in RAW_TOKENS TOKENS_TOO_SHORT TOKENS_STOPWORD TOKENS_KEPT TITLE_TOKENS_KEPT DOCS_WITHOUT_TOKENS POSTINGS_EMITTED; do chk $c "$C2"; done
  for c in TERMS_TOTAL TERMS_MAIN_INDEX TERMS_CORPUS_STOP TERMS_RARE; do chk $c "$C2"; done
  for c in DOCS_CANDIDATE DOCS_MISSING_A_WORD DOCS_WORDS_NOT_ADJACENT DOCS_MATCHED PHRASE_OCCURRENCES; do chk $c "$C3"; done
  cat "$VERIF"
  [[ $RC -eq 0 ]] && ok "VERIFY: Hadoop results equal the independent reference" || warn "VERIFY: differences found - see $VERIF"
else
  warn "python3 not found - verification skipped (results are NOT verified)"
fi

step "8. Wrap-up"
yarn_apps | tee "$LOG/q2_yarn_applications.txt" || true
grep -h '^@@JOB' "$LOG"/q2_*.log 2>/dev/null | awk -F'\t' 'BEGIN{print "tag\tjob_id\tsuccess\telapsed_ms\ttracking_url"} {print $2"\t"$3"\t"$4"\t"$5"\t"$6}' > "$OUT/q2_jobs_summary.tsv" || true
echo
echo "Viva helper - another phrase on the existing index (no rebuild needed):"
echo "  hadoop jar $JAR $MAIN -Dphrase=\"data analytics\" --stage phrase --index-dir $H_OUT/main/index --output $H_OUT/viva"
echo "Results in HDFS : $H_OUT/main/index (part-r-*, corpus_stop-r-*, rare-r-*)  and  $H_OUT/main/phrase"
echo "Local files     : $OUT   (counters in $OUT/metrics, logs in $LOG)"
ok "Q2 finished."
