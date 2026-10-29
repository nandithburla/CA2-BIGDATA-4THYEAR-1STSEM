package ca2.q3;

/** Job 1 counters (reduce-side join of orders and returns). */
public enum JoinCounters {
  ORDER_LINES_READ, ORDER_HEADERS_SKIPPED, BAD_ORDER_LINES,
  RETURN_LINES_READ, RETURN_HEADERS_SKIPPED, BAD_RETURN_LINES,
  ORDER_LINES_CANCELLED,        // excluded from everything downstream
  ORDER_LINES_KEPT,
  DUPLICATE_ORDER_LINES,        // a second order record for the same (order_id, line_no): ignored
  LINES_WITH_RETURNS,
  LINES_WITH_MULTIPLE_RETURNS,  // one order line returned in two or more separate returns
  RETURNS_MATCHED,
  RETURNS_ON_CANCELLED,         // return of a cancelled line: dropped (the order exists, so it is not an orphan)
  RETURNS_ORPHAN,               // no order line with that (order_id, line_no) -> MultipleOutputs orphan_returns
  RETURN_EXCEEDS_ORDER          // sum of returned quantities > ordered quantity: net_qty capped at 0 returned excess
}
