package ca2.q5;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Job 1 reducer: FULL OUTER JOIN gateway_txns (ONE) x bank_settlements (MANY) on txn_id.
 * Secondary sort delivers the transaction first; the settlements are STREAMED - only a running sum and a count are kept (no buffering).
 *   transaction present  -> one output record per transaction:  order_id,txn_id,amount_cents,currency,status,txn_ts,settled_micro,settlement_count
 *   no transaction       -> every settlement of the group is an ORPHAN_SETTLEMENT (MultipleOutputs "orphansettlement")
 */
public class TxnJoinReducer extends Reducer<TaggedKey, Text, NullWritable, Text> {
  public static final String ORPHANS = "orphansettlement";
  private MultipleOutputs<NullWritable, Text> mos;
  private final Text out = new Text();

  @Override
  protected void setup(Context ctx) { mos = new MultipleOutputs<>(ctx); }

  @Override
  protected void reduce(TaggedKey key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
    String txnId = key.getId().toString();         // the key object mutates while iterating
    boolean haveTxn = false;
    String txnRest = "";
    long settledMicro = 0;
    int n = 0;
    for (Text v : values) {
      if (key.getTag() == TaggedKey.ONE) {
        if (haveTxn) { ctx.getCounter(TxnJoinCounters.DUPLICATE_TXN_RECORDS).increment(1); continue; }
        haveTxn = true;
        txnRest = v.toString();
      } else {
        String[] s = v.toString().split(",", -1);  // settlement_id,settled_amount_inr,settlement_date
        if (!haveTxn) {
          ctx.getCounter(TxnJoinCounters.ORPHAN_SETTLEMENTS).increment(1);
          out.set("ORPHAN_SETTLEMENT," + s[0] + ",-,INR,0.00,0.000000,0.000000," + Money.micro(Money.inrCentsToMicro(s[1])) + ",0," + txnId + "," + s[2]);
          mos.write(ORPHANS, NullWritable.get(), out, "orphan_settlement");
        } else {
          ctx.getCounter(TxnJoinCounters.SETTLEMENTS_MATCHED).increment(1);
          settledMicro += Money.inrCentsToMicro(s[1]);
          n++;
        }
      }
    }
    if (!haveTxn) return;
    ctx.getCounter(TxnJoinCounters.TXNS_JOINED).increment(1);
    ctx.getCounter(n > 0 ? TxnJoinCounters.TXNS_WITH_SETTLEMENT : TxnJoinCounters.TXNS_WITHOUT_SETTLEMENT).increment(1);
    if (n > 1) ctx.getCounter(TxnJoinCounters.TXNS_SETTLED_MULTIPLE_TIMES).increment(1);
    int c1 = txnRest.indexOf(',');
    out.set(txnRest.substring(0, c1) + "," + txnId + "," + txnRest.substring(c1 + 1) + "," + settledMicro + "," + n);
    ctx.write(NullWritable.get(), out);
  }

  @Override
  protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }
}
