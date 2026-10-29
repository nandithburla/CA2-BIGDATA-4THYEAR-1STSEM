package ca2.q3;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** orders.csv: order_id,line_no,order_ts,customer_id,product_id,qty,unit_price,status  ->  key (order_id,line_no,0), value "month,product,qty,price_cents,status". */
public class OrderMapper extends Mapper<Object, Text, JoinKey, Text> {
  private final JoinKey key = new JoinKey();
  private final Text val = new Text();

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    String line = value.toString().trim();
    if (line.isEmpty()) return;
    String[] f = line.split(",", -1);
    if (f[0].equals("order_id")) { ctx.getCounter(JoinCounters.ORDER_HEADERS_SKIPPED).increment(1); return; }
    ctx.getCounter(JoinCounters.ORDER_LINES_READ).increment(1);
    try {
      if (f.length != 8) throw new IllegalArgumentException("fields");
      String orderId = f[0].trim(), ts = f[2].trim(), product = f[4].trim(), status = f[7].trim().toUpperCase(java.util.Locale.ROOT);
      int lineNo = Integer.parseInt(f[1].trim());
      long qty = Long.parseLong(f[5].trim());
      long cents = Money.parseCents(f[6]);
      if (orderId.isEmpty() || product.isEmpty() || qty <= 0 || cents < 0 || !ts.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) throw new IllegalArgumentException("value");
      key.set(orderId, lineNo, JoinKey.ORDER);
      val.set(ts.substring(0, 7) + "," + product + "," + qty + "," + cents + "," + status);
      ctx.write(key, val);
    } catch (RuntimeException e) {
      ctx.getCounter(JoinCounters.BAD_ORDER_LINES).increment(1);
    }
  }
}
