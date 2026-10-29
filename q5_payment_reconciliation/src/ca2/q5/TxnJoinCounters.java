package ca2.q5;

/** Job 1 counters: reduce-side full outer join of gateway transactions and bank settlements on txn_id. */
public enum TxnJoinCounters {
  TXN_LINES_READ, TXN_HEADERS_SKIPPED, BAD_TXN_LINES, UNKNOWN_TXN_STATUS,
  SETTLEMENT_LINES_READ, SETTLEMENT_HEADERS_SKIPPED, BAD_SETTLEMENT_LINES,
  TXNS_JOINED,                  // transactions written to the next job
  DUPLICATE_TXN_RECORDS,        // second record with the same txn_id: ignored
  TXNS_WITH_SETTLEMENT, TXNS_WITHOUT_SETTLEMENT,
  TXNS_SETTLED_MULTIPLE_TIMES,  // "a transaction settled twice"
  SETTLEMENTS_MATCHED,
  ORPHAN_SETTLEMENTS            // settlement whose txn_id is not in the gateway file
}
