package ca2.q1;

/** Job 3 (mean + 2 sigma outliers, replicated join) custom counters. */
public enum OutlierCounters {
  /** statistics rows loaded from the distributed cache in Mapper.setup() */
  STATS_ROWS_LOADED,
  /** clean readings seen by the mappers */
  READINGS_SEEN,
  /** reading whose (city, meter, month) was missing from the side file (should be 0) */
  LOOKUP_MISS,
  /** malformed clean line (should be 0) */
  MALFORMED_CLEAN_LINES,
  /** readings with kWh > mean + 2 sigma */
  OUTLIER_READINGS,
  /** meter-months written */
  METER_MONTHS,
  /** meter-months with at least one outlier */
  METER_MONTHS_WITH_OUTLIERS
}
