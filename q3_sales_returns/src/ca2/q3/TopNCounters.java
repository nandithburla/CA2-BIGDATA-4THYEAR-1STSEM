package ca2.q3;

/** Job 3 counters (Top-N). */
public enum TopNCounters {
  PRODUCTS_LOADED,
  CANDIDATES_SEEN,              // aggregate rows read by the mappers
  CANDIDATES_EVICTED_IN_MAPPER, // rows dropped by the bounded (<= N) structure inside the mappers
  CANDIDATES_EMITTED,           // rows sent to the reducers (<= groups x N per mapper)
  GROUPS,                       // (category, month) groups
  ROWS_WRITTEN,
  TIES_ON_REVENUE,              // neighbouring ranks with identical revenue (kept thanks to the strict total order)
  REDUCERS_WITH_OUTPUT,         // number of reducers that received at least one category
  PRODUCT_NAME_UNKNOWN
}
