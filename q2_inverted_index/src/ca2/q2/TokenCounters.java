package ca2.q2;

/** Job 2 mapper counters (filtering pattern: every filtered token is counted by reason). */
public enum TokenCounters {
  STOPWORDS_LOADED,
  RAW_TOKENS,           // tokens produced by the split on non-alphanumeric characters
  TOKENS_TOO_SHORT,     // length < 2
  TOKENS_STOPWORD,      // dropped because they are in stopwords.txt (Distributed Cache)
  TOKENS_KEPT,
  TITLE_TOKENS_KEPT,
  DOCS_WITHOUT_TOKENS,  // nothing left after filtering
  POSTINGS_EMITTED
}
