package ca2.q2;

/** Job 2 reducer counters. */
public enum IndexCounters {
  TERMS_TOTAL,
  TERMS_MAIN_INDEX,
  TERMS_CORPUS_STOP,    // df > 0.4 N
  TERMS_RARE,           // df = 1
  POSTINGS_MAIN_INDEX,
  TERMS_DF_EQUALS_N     // idf = 0
}
