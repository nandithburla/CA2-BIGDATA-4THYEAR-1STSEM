package ca2.q3;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Job 1 reducer - reduce-side join of orders and returns on (order_id, line_no).
 * Secondary sort guarantees the order record (tag 0) comes first; the returns (tag 1) are STREAMED: only a running sum is kept.
 *   cancelled order line      -> excluded (its returns are counted and dropped)
 *   no order record           -> every return of the group goes to MultipleOutputs "orphan_returns"
 *   net_qty = qty - sum(qty_returned), capped at 0 (excess counted once per line in RETURN_EXCEEDS_ORDER)
 * Output line: order_id,line_no,month,product_id,qty,returned,net_qty,unit_price_cents,net_revenue_cents
 */
public class JoinReducer extends Reducer<JoinKey, Text, NullWritable, Text> {
  public static final String ORPHANS = "orphanreturns";
  private MultipleOutputs<NullWritable, Text> mos;
  private final Text out = new Text();

  @Override
  protected void setup(Context ctx) { mos = new MultipleOutputs<>(ctx); }

  @Override
  protected void reduce(JoinKey key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
    boolean haveOrder = false, cancelled = false;
    String month = "", product = "";
    long qty = 0, cents = 0, returned = 0;
    int nReturns = 0;
    String orderId = key.getOrderId().toString();    // the key object is reused/mutated while we iterate: take what we need now
    int lineNo = key.getLineNo();

    for (Text v : values) {
      if (key.getTag() == JoinKey.ORDER) {
        if (haveOrder) { ctx.getCounter(JoinCounters.DUPLICATE_ORDER_LINES).increment(1); continue; }
        haveOrder = true;
        String[] f = v.toString().split(",", -1);   // month,product,qty,cents,status
        month = f[0]; product = f[1]; qty = Long.parseLong(f[2]); cents = Long.parseLong(f[3]);
        cancelled = f[4].equals("CANCELLED");
        if (cancelled) ctx.getCounter(JoinCounters.ORDER_LINES_CANCELLED).increment(1);
        else ctx.getCounter(JoinCounters.ORDER_LINES_KEPT).increment(1);
      } else {
        String[] r = v.toString().split(",", -1);   // return_id,qty_returned,return_ts
        if (!haveOrder) {
          ctx.getCounter(JoinCounters.RETURNS_ORPHAN).increment(1);
          out.set(r[0] + "," + orderId + "," + lineNo + "," + r[1] + "," + r[2]);
          mos.write(ORPHANS, NullWritable.get(), out, "orphan_returns");
        } else if (cancelled) {
          ctx.getCounter(JoinCounters.RETURNS_ON_CANCELLED).increment(1);
        } else {
          ctx.getCounter(JoinCounters.RETURNS_MATCHED).increment(1);
          returned += Long.parseLong(r[1]);
          nReturns++;
        }
      }
    }
    if (!haveOrder || cancelled) return;
    if (nReturns > 0) ctx.getCounter(JoinCounters.LINES_WITH_RETURNS).increment(1);
    if (nReturns > 1) ctx.getCounter(JoinCounters.LINES_WITH_MULTIPLE_RETURNS).increment(1);
    long net = qty - returned;
    if (net < 0) { ctx.getCounter(JoinCounters.RETURN_EXCEEDS_ORDER).increment(1); net = 0; }
    out.set(orderId + "," + lineNo + "," + month + "," + product + "," + qty + "," + returned + "," + net + "," + cents + "," + (net * cents));
    ctx.write(NullWritable.get(), out);
  }

  @Override
  protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }
}
