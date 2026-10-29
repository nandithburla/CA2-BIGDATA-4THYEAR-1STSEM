# Q2 – Positional inverted index, TF-IDF ranking and phrase search

Package `ca2.q2` · one driver `Q2Driver` (Tool/ToolRunner) · 3 chained jobs · Java 11 · `org.apache.hadoop.mapreduce` only.

## Run
```bash
cd q2_inverted_index
./run_q2.sh                       # phrase "big data analytics"
Q2_PHRASE="hadoop yarn" ./run_q2.sh --yes     # another 2-3 word phrase (viva)
./run_q2.sh --help
```
Outputs: `output/` (index, corpus_stop, rare, phrase result, counters, planted-case evidence, delta comparison, verification), `logs/`, `output/metrics/`.
`>>> SCREENSHOT Q2-x` lines tell you what to capture.

## Pipeline
| Job | What | Key → Value |
|---|---|---|
| J1 `Q2-J1-count-docs` | map-only: validate lines, **custom counter `DocCounters.DOCS` = N**, write valid lines | `NullWritable → Text` |
| J2 `Q2-J2-index-delta-*` | tokenise (title+body = ONE stream, positions from 0), filter short/stop-words (**stopwords.txt via Distributed Cache**), tf (title 3, body 1), tf-idf; **N read from the Configuration (`q2.num.docs`)** | `Text(term) → PostingWritable(doc_id, tf, positions)` |
| J3 `Q2-J3-phrase` | phrase search on the **index only** (`-Dphrase="…"`, 2–3 words), 1 reducer → global ranking | `Text(doc_id) → Text("term:p1,p2,…")` |

* Output line (J2): `term<TAB>df<TAB>doc_id:tf:tfidf:[p1,p2,…] doc_id:…` – postings sorted by tfidf desc then doc_id asc; `tfidf = tf × log10(N/df)` rounded HALF_UP to 4 decimals
  (the sort uses the rounded value). Positions are printed as absolute positions; the *shuffled* Writable stores gaps (`[3,10,25]` → `[3,7,15]`) with `WritableUtils.writeVInt`.
* `df > 0.4·N` → `MultipleOutputs` file **`corpus_stop-r-*`**; `df = 1` → **`rare-r-*`**; neither is in the main index (`part-r-*`). Both special files use the same full line format.
* Job 3 reads the whole index written by Job 2 (`part-r-*` and the two special files, so a phrase containing a very common/rare word still works);
  `-Dq2.phrase.main.only=true` restricts it to the main index. Words that are stop-words/too short are rejected with an error (exit 2) – they are not in the index.
* Position policy (`-Dq2.positions=kept|raw`, default **kept**): positions are numbered over the token stream left after filtering. `raw` numbers all tokens (stop-words leave gaps).
* Custom counters (enums): `DocCounters`, `TokenCounters`, `IndexCounters`, `PhraseCounters`.

## Is a Combiner applicable in Job 2?  No
Each `(term, doc)` pair is produced once (the mapper already aggregates per document) so a combiner has nothing to merge; df and the global sort are not
associative aggregations; a combiner must keep the value type – concatenating postings would only add bytes and break the count of postings used as df.
(`run_q2.sh` prints this and saves it in `output/q2_combiner_answer.txt`.)

## Delta encoding result (honest note)
`run_q2.sh` runs Job 2 three times: gap VInts (required), absolute VInts and absolute 4-byte ints, and tabulates map-output / shuffle bytes.
The largest position in this corpus is 111, which a VInt stores in **one byte whether it is a gap or an absolute value** – so on *this* data gaps cannot
beat absolute VInts (the two columns are equal) but both beat fixed ints. `ca2.q2.DeltaDemo` shows the break-even for long documents with a synthetic example (52 vs 130 bytes).
Put this reasoning in the report instead of claiming a saving that does not exist.

## Verified vs not verified
Verified here (simulator of the Hadoop API + independent Python reference, real dataset): all four result sets and all counters equal the reference; delta/vint/fixed give identical indexes.
**Not verified here:** compilation against real Hadoop 3.3.6, Maven, YARN execution, JobHistory.
