package ca2.q3;

/** Job 2 counters (aggregation with a replicated join on products.csv). */
public enum RevenueCounters {
  PRODUCTS_LOADED, PRODUCTS_BAD_ROWS, PRODUCTS_DUPLICATE_IDS,
  JOINED_LINES_READ, BAD_JOINED_LINES,
  LINES_UNMAPPED_PRODUCT,       // product_id not in products.csv -> category UNMAPPED
  AGGREGATE_KEYS                // (category, month, product_id) groups written
}
