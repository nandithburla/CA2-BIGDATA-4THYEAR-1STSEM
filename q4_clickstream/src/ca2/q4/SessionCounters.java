package ca2.q4;

/** Job 1 counters (sessionization). */
public enum SessionCounters {
  LINES_READ, MALFORMED_LINES, BAD_TIMESTAMP, EMPTY_USER_OR_URL,
  EVENTS,                       // valid events
  USERS,
  USERS_OVER_100K_EVENTS,       // heavy hitters (the 300 000-event user)
  SESSIONS,
  SESSIONS_NORMAL,
  SESSIONS_BOT,
  BOT_BY_EVENT_COUNT,           // more than 500 events
  BOT_BY_AVG_GAP,               // average inter-event gap below 1 second
  SPLIT_BY_GAP,                 // new session because the gap was > 30 minutes
  SPLIT_BY_MIDNIGHT_IST,        // new session because the IST calendar date changed
  GAP_EXACTLY_30_MIN,           // gap == 30:00.000 -> same session (boundary case)
  IDENTICAL_TIMESTAMP_EVENTS    // two consecutive events of one user with the same timestamp (gap 0)
}
