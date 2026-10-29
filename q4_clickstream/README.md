# Q4 – Clickstream sessionization (secondary sort) and conversion funnel

Package `ca2.q4` · one driver `Q4Driver` (Tool/ToolRunner) · 2 chained jobs · Java 11 · `org.apache.hadoop.mapreduce` only.

## Run
```bash
cd q4_clickstream
./run_q4.sh            # reducer memory capped: mapreduce.reduce.memory.mb=512, heap -Xmx384m (Q4_REDUCE_MEMORY_MB / Q4_REDUCE_HEAP_MB in config/ca2.env)
./run_q4.sh --yes --pause
```
Outputs in `output/`: `q4_sessions.csv`, `q4_bots.csv`, `q4_funnel.csv`, `q4_reducer_memory.txt`, counters, planted-case evidence, verification. Logs in `logs/`.

## Pipeline
| Job | Pattern | Details |
|---|---|---|
| J1 `Q4-J1-sessionize-secondary-sort` | **secondary sort**, MultipleOutputs | key `ClickKey(user_id, event_ts[, url])`; `UserPartitioner` (user_id only); `ClickSortComparator` (user_id, event_ts); `UserGroupingComparator` (user_id only); **streaming** `SessionReducer` |
| J2 `Q4-J2-funnel-per-date` | summarization + Combiner | funnel facts `date TAB stage` of **non-bot** sessions → per IST date: sessions, #reached stage 1..4, stage-to-stage % |

Session rules: new session when `gap > 30 min` **strictly** (exactly 30:00 stays in the session) **or** the IST calendar date changes (IST = UTC+05:30, no DST).
Funnel stages are matched greedily in order inside one session: `/home` → `/product/*` → `/cart` → `/checkout/success` (a stage counts only if all earlier stages occurred earlier in the same session).
Output: `user_id,session_seq,start_ts,end_ts,duration_sec,page_views,distinct_urls,entry_url,exit_url` (ISO-8601 with `+05:30`; `session_seq` counts all sessions of the user, bots included).
Bot session = more than 500 events **or** average inter-event gap < 1 s → `bots-r-*` (extra last column = reason), excluded from sessions output and funnel.

## Why the reducer is streaming (and the memory proof)
`SessionReducer` never stores a user's events: it keeps only the current session's counters, first/last url, funnel stage and the set of **distinct urls of the current session**
(bounded by the catalogue – ≈5 000 entries for the 300 000-event user – and cleared at every session end; it is not a collection of events).
`run_q4.sh` runs the job with `-Dmapreduce.reduce.memory.mb=512 -Dmapreduce.reduce.java.opts=-Xmx384m`; every reducer writes `reducer_memory-r-*` with the heap limit really in force, a sampled peak heap and the
number of events it streamed (the reducer that handled `largest_user_events=300000` is the proof line). Screenshot the JobHistory *Configuration* page (both properties) as well.
Note: YARN rounds container requests up to `yarn.scheduler.minimum-allocation-mb` (default 1024), so the `-Xmx` setting is what makes the cap effective.

## Interpretation notes you must know for the viva
1. **Identical timestamps.** Two events of one user with the same timestamp are ordered by url (ascending) – the url is a third key field used *only* as a tie-break so the output is identical on every run (`-Dq4.tiebreak=none` switches it off, ordering of ties is then undefined).
2. **U9000004 (two events with the same timestamp)** has an average gap of 0 s, so the rule "average inter-event gap below 1 second", taken literally, makes it a **bot session** (it is the only such planted case; the real bot is U9999999).
   If you prefer to treat it as a normal 2-event session, run with `-Dq4.bot.min.events=3` (the gap rule then needs ≥ 3 events). `run_q4.sh` shows both outcomes. Choose one and state it in the report.
3. Bot sessions are not repeated in the sessions output (they are only in the `bots` file). 

## Verified vs not verified
Verified here (Hadoop-API simulator + independent Python reference, real dataset): sessions, bots, funnel and all counters equal the reference.
**Not verified here:** real Hadoop 3.3.6 compilation, Maven, YARN, the 512 MB container behaviour, JobHistory.
