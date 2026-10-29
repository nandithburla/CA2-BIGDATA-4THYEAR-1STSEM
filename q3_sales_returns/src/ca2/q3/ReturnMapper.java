package ca2.q3;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** returns.csv: return_id,order_id,line_no,qty_returned,return_ts  ->  key (order_id,line_no,1), value "return_id,qty_returned,return_ts". */
public class ReturnMapper extends Mapper<Object, Text, JoinKey, Text> {
  private final JoinKey key = new JoinKey();
  private final Text val = new Text();

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    String line = value.toString().trim();
    if (line.isEmpty()) return;
    String[] f = line.split(",", -1);
    if (f[0].equals("return_id")) { ctx.getCounter(JoinCounters.RETURN_HEADERS_SKIPPED).increment(1); return; }
    ctx.getCounter(JoinCounters.RETURN_LINES_READ).increment(1);
    try {
      if (f.length != 5) throw new IllegalArgumentException("fields");
      String orderId = f[1].trim();
      int lineNo = Integer.parseInt(f[2].trim());
      long q = Long.parseLong(f[3].trim());
      if (orderId.isEmpty() || f[0].trim().isEmpty() || q <= 0) throw new IllegalArgumentException("value");
      key.set(orderId, lineNo, JoinKey.RETURN);
      val.set(f[0].trim() + "," + q + "," + f[4].trim());
      ctx.write(key, val);
    } catch (RuntimeException e) {
      ctx.getCounter(JoinCounters.BAD_RETURN_LINES).increment(1);
    }
  }
}
