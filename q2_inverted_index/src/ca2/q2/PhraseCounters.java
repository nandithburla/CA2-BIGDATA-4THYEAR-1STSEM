package ca2.q2;

/** Job 3 counters. */
public enum PhraseCounters {
  INDEX_LINES_READ,
  INDEX_LINES_WITH_PHRASE_WORD,
  POSTINGS_PARSED,
  DOCS_CANDIDATE,                // documents that contain at least one phrase word
  DOCS_MISSING_A_WORD,
  DOCS_WORDS_NOT_ADJACENT,       // every word is present but never at consecutive positions
  DOCS_MATCHED,
  PHRASE_OCCURRENCES
}
