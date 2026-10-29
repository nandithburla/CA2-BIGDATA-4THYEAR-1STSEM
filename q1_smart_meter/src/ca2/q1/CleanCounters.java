package ca2.q1;

/** Job 1 (clean + de-duplicate) custom counters. Every rejection category has its own counter. */
public enum CleanCounters {
  /** line did not have exactly 4 comma separated fields (includes blank lines) */
  REJ_MALFORMED_FIELDS,
  /** meter_id or city empty */
  REJ_EMPTY_METER_OR_CITY,
  /** reading_ts is not a valid yyyy-MM-dd HH:mm:ss timestamp (e.g. 14/03/2026 18:30) */
  REJ_BAD_TIMESTAMP,
  /** kwh field empty */
  REJ_EMPTY_KWH,
  /** kwh field not a number (abc, NaN ...) */
  REJ_NON_NUMERIC_KWH,
  /** kwh < 0 */
  REJ_NEGATIVE_KWH,
  /** records that passed validation (before de-duplication) */
  VALID_RECORDS,
  /** (meter_id, reading_ts) groups that occurred more than once */
  DUP_KEY_GROUPS,
  /** records removed because another record with the same (meter_id, reading_ts) was kept */
  DUP_RECORDS_DROPPED,
  /** duplicate groups whose copies came from two or more different input files (different mappers) */
  DUP_GROUPS_CROSS_FILE,
  /** duplicate groups whose copies were NOT identical (different city and/or kWh) */
  DUP_GROUPS_CONFLICTING,
  /** records written to the clean data set */
  CLEAN_RECORDS
}
