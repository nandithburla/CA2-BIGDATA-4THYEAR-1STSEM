package ca2.q5;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Job 2 reducer: FULL OUTER JOIN orders (ONE) x joined transactions (MANY) on order_id, then classification.
 * The order record arrives first (secondary sort); the transactions are STREAMED - only running sums (micro-INR) and two flags are kept.
 * Everything is integer micro-INR (cents x rate in 1e-4), so the result does not depend on the arrival order of the transactions.
 *
 * Rules, applied in this order (net_paid = sum SUCCESS - sum REFUNDED, every amount converted at the rate of the ORDER date):
 *   1 UNPAID  net_paid == 0     2 UNDERPAID / OVERPAID  |net_paid - order_amount| > Rs 1.00
 *   3 PAID_NOT_SETTLED  some SUCCESS txn has no settlement   4 SETTLEMENT_MISMATCH  |sum settled - net_paid| > Rs 1.00   5 MATCHED
 * No order for the group -> every transaction is an ORPHAN_TXN (converted at its own date).
 * Output line (all categories): category,id,customer_id,currency,order_amount,order_inr,net_paid_inr,settled_inr,txn_count,ref1,ref2
 */
public class ReconReducer extends Reducer<TaggedKey, Text, NullWritable, Text> {
  static final long TOLERANCE_MICRO = 1_000_000L;     // Rs 1.00
  public static final String OUT = "category";
  private MultipleOutputs<NullWritable, Text> mos;
  private FxTable fx;
  private final Text out = new Text();

  @Override
  protected void setup(Context ctx) throws IOException {
    mos = new MultipleOutputs<>(ctx);
    fx = FxTable.load(ctx);
  }

  private void emit(String cat, String line) throws IOException, InterruptedException {
    out.set(cat + "," + line);
    mos.write(OUT, NullWritable.get(), out, cat.toLowerCase(java.util.Locale.ROOT) + "/part");
  }

  @Override
  protected void reduce(TaggedKey key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
    String orderId = key.getId().toString();
    boolean haveOrder = false;
    String customer = "", cur = "", amountText = "", date = "";
    long orderInr = 0, net = 0, settled = 0;
    int txns = 0;
    boolean anyFailed = false, successUnsettled = false;

    for (Text v : values) {
      if (key.getTag() == TaggedKey.ONE) {
        if (haveOrder) { ctx.getCounter(ReconCounters.DUPLICATE_ORDER_RECORDS).increment(1); continue; }
        haveOrder = true;
        String[] o = v.toString().split(",", -1);       // customer_id,order_amount_cents,currency,order_ts,order_amount_text
        customer = o[0]; cur = o[2]; date = o[3].substring(0, 10); amountText = o[4];
        long r = fx.rate(cur, date, ctx);
        orderInr = r < 0 ? 0 : Long.parseLong(o[1]) * r;
        continue;
      }
      String[] t = v.toString().split(",", -1);         // txn_id,cents,currency,status,txn_ts,settled_micro,settlement_count
      long cents = Long.parseLong(t[1]);
      long settledMicro = Long.parseLong(t[5]);
      int n = Integer.parseInt(t[6]);
      if (!haveOrder) {                                  // ORPHAN_TXN: converted at the transaction's own date
        long r = fx.rate(t[2], t[4].substring(0, 10), ctx);
        long inr = r < 0 ? 0 : cents * r;
        long signed = t[3].equals("SUCCESS") ? inr : t[3].equals("REFUNDED") ? -inr : 0;
        ctx.getCounter(ReconCounters.CAT_ORPHAN_TXN).increment(1);
        emit("ORPHAN_TXN", t[0] + ",-," + t[2] + "," + Money.micro(cents * 10_000L) + ",0.000000," + Money.micro(signed)
            + "," + Money.micro(settledMicro) + "," + n + "," + orderId + "," + t[4]);
        continue;
      }
      txns++;
      long r = fx.rate(t[2], date, ctx);
      long inr = r < 0 ? 0 : cents * r;
      settled += settledMicro;
      switch (t[3]) {
        case "SUCCESS":  net += inr; ctx.getCounter(ReconCounters.TXNS_SUCCESS).increment(1); if (n == 0) successUnsettled = true; break;
        case "REFUNDED": net -= inr; ctx.getCounter(ReconCounters.TXNS_REFUNDED).increment(1); break;
        default:         anyFailed = true; ctx.getCounter(ReconCounters.TXNS_FAILED).increment(1);
      }
    }
    if (!haveOrder) return;

    ctx.getCounter(ReconCounters.ORDERS).increment(1);
    if (txns == 0) ctx.getCounter(ReconCounters.ORDERS_WITHOUT_TXN).increment(1);
    if (anyFailed) ctx.getCounter(ReconCounters.ORDERS_WITH_FAILED_TXN).increment(1);

    String cat;
    if (net == 0) cat = "UNPAID";
    else if (Math.abs(net - orderInr) > TOLERANCE_MICRO) cat = net < orderInr ? "UNDERPAID" : "OVERPAID";
    else if (successUnsettled) cat = "PAID_NOT_SETTLED";
    else if (Math.abs(settled - net) > TOLERANCE_MICRO) cat = "SETTLEMENT_MISMATCH";
    else cat = "MATCHED";
    ctx.getCounter(ReconCounters.valueOf("CAT_" + cat)).increment(1);
    emit(cat, orderId + "," + customer + "," + cur + "," + amountText + "," + Money.micro(orderInr) + "," + Money.micro(net) + ","
        + Money.micro(settled) + "," + txns + ",-,-");
  }

  @Override
  protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }
}
