package ca2.q5;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** gateway_txns.csv: txn_id,order_id,amount,currency,status,txn_ts  ->  key (txn_id, ONE), value "order_id,amount_cents,currency,status,txn_ts". */
public class TxnMapper extends Mapper<Object, Text, TaggedKey, Text> {
  private final TaggedKey key = new TaggedKey();
  private final Text val = new Text();

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    String line = value.toString().trim();
    if (line.isEmpty()) return;
    String[] f = line.split(",", -1);
    if (f[0].equals("txn_id")) { ctx.getCounter(TxnJoinCounters.TXN_HEADERS_SKIPPED).increment(1); return; }
    ctx.getCounter(TxnJoinCounters.TXN_LINES_READ).increment(1);
    try {
      if (f.length != 6 || f[0].trim().isEmpty() || f[1].trim().isEmpty()) throw new IllegalArgumentException();
      long cents = Money.cents(f[2]);
      String status = f[4].trim().toUpperCase(java.util.Locale.ROOT);
      if (!status.equals("SUCCESS") && !status.equals("FAILED") && !status.equals("REFUNDED")) {
        ctx.getCounter(TxnJoinCounters.UNKNOWN_TXN_STATUS).increment(1);
        ctx.getCounter(TxnJoinCounters.BAD_TXN_LINES).increment(1);
        return;
      }
      if (cents < 0 || f[3].trim().isEmpty()) throw new IllegalArgumentException();
      key.set(f[0].trim(), TaggedKey.ONE);
      val.set(f[1].trim() + "," + cents + "," + f[3].trim() + "," + status + "," + f[5].trim());
      ctx.write(key, val);
    } catch (RuntimeException e) {
      ctx.getCounter(TxnJoinCounters.BAD_TXN_LINES).increment(1);
    }
  }
}
