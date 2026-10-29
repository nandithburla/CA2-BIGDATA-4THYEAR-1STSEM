package ca2.q1;

/** Job 2 (statistics) custom counters. */
public enum StatsCounters {
  /** clean input line that did not have 4 fields (should be 0 - Job 1 already cleaned the data) */
  MALFORMED_CLEAN_LINES,
  /** kWh value had more decimals than the fixed-point scale (rounded half-up) */
  KWH_ROUNDED_TO_SCALE,
  /** (city, meter, month) groups written */
  STAT_GROUPS,
  /** groups with exactly one reading (sigma = 0 by definition) */
  SINGLE_READING_GROUPS,
  /** groups whose variance came out negative before the guard was applied */
  NEGATIVE_VARIANCE_CLAMPED,
  /** groups with population standard deviation exactly 0 */
  ZERO_SIGMA_GROUPS
}
