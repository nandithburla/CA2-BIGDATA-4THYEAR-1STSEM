package ca2.q5;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** orders.csv: order_id,customer_id,order_amount,currency,order_ts -> key (order_id, ONE), value "customer_id,order_amount_cents,currency,order_ts,order_amount_text". */
public class OrderMapper extends Mapper<Object, Text, TaggedKey, Text> {
  private final TaggedKey key = new TaggedKey();
  private final Text val = new Text();

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    String line = value.toString().trim();
    if (line.isEmpty()) return;
    String[] f = line.split(",", -1);
    if (f[0].equals("order_id")) { ctx.getCounter(ReconCounters.ORDER_HEADERS_SKIPPED).increment(1); return; }
    ctx.getCounter(ReconCounters.ORDER_LINES_READ).increment(1);
    try {
      if (f.length != 5 || f[0].trim().isEmpty() || f[3].trim().isEmpty() || !f[4].trim().matches("\\d{4}-\\d{2}-\\d{2} .*")) throw new IllegalArgumentException();
      long cents = Money.cents(f[2]);
      if (cents < 0) throw new IllegalArgumentException();
      key.set(f[0].trim(), TaggedKey.ONE);
      val.set(f[1].trim() + "," + cents + "," + f[3].trim() + "," + f[4].trim() + "," + f[2].trim());
      ctx.write(key, val);
    } catch (RuntimeException e) {
      ctx.getCounter(ReconCounters.BAD_ORDER_LINES).increment(1);
    }
  }
}
