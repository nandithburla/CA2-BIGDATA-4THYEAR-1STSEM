package ca2.q5;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Output of Job 1: order_id,txn_id,amount_cents,currency,status,txn_ts,settled_micro,settlement_count -> key (order_id, MANY), value "txn_id,...". */
public class JoinedTxnMapper extends Mapper<Object, Text, TaggedKey, Text> {
  private final TaggedKey key = new TaggedKey();
  private final Text val = new Text();

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    String line = value.toString();
    int c = line.indexOf(',');
    if (c <= 0) return;
    ctx.getCounter(ReconCounters.TXN_RECORDS_READ).increment(1);
    key.set(line.substring(0, c), TaggedKey.MANY);
    val.set(line.substring(c + 1));
    ctx.write(key, val);
  }
}
