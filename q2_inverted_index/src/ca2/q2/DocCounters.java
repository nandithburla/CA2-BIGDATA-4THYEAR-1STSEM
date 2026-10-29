package ca2.q2;

/** Job 1 counters. DOCS is the number N of documents that is handed to Job 2. */
public enum DocCounters {
  DOCS,                 // N - every well-formed document (also those with an empty body)
  MALFORMED_LINES,      // fewer than 3 tab-separated fields
  EMPTY_DOC_ID,         // first field empty
  EMPTY_BODY_DOCS,      // body field empty (document is kept: its title still produces tokens)
  EMPTY_TITLE_DOCS,
  EXTRA_TAB_LINES       // more than 3 fields: the surplus tabs are kept inside the body
}
