package ca2.q4;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Job 2 reducer: per IST date  date,sessions,home,product,cart,success,home_to_product_pct,product_to_cart_pct,cart_to_success_pct
 * (stage-to-stage conversion in %, 2 decimals HALF_UP; NA when the previous stage is 0).
 */
public class FunnelReducer extends Reducer<Text, StageCounts, NullWritable, Text> {
  private final Text out = new Text();

  @Override
  protected void reduce(Text date, Iterable<StageCounts> values, Context ctx) throws IOException, InterruptedException {
    StageCounts s = new StageCounts();
    for (StageCounts x : values) s.add(x);
    out.set(date + "," + s.v[0] + "," + s.v[1] + "," + s.v[2] + "," + s.v[3] + "," + s.v[4] + ","
        + pct(s.v[2], s.v[1]) + "," + pct(s.v[3], s.v[2]) + "," + pct(s.v[4], s.v[3]));
    ctx.write(NullWritable.get(), out);
    ctx.getCounter(FunnelCounters.DATES).increment(1);
  }

  static String pct(long next, long prev) {
    if (prev == 0) return "NA";
    return new BigDecimal(100L * next).divide(new BigDecimal(prev), 2, RoundingMode.HALF_UP).toPlainString();
  }
}
