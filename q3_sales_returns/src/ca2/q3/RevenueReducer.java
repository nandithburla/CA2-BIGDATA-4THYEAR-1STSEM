package ca2.q3;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Job 2 reducer: net units and net revenue per (category, month, product). Output: category,month,product_id,net_units,net_revenue */
public class RevenueReducer extends Reducer<CatMonthProductKey, AggWritable, NullWritable, Text> {
  private final Text out = new Text();

  @Override
  protected void reduce(CatMonthProductKey key, Iterable<AggWritable> values, Context ctx) throws IOException, InterruptedException {
    long u = 0, c = 0;
    for (AggWritable a : values) { u += a.getUnits(); c += a.getCents(); }
    out.set(key.getCategory() + "," + key.getMonth() + "," + key.getProduct() + "," + u + "," + Money.format(c));
    ctx.write(NullWritable.get(), out);
    ctx.getCounter(RevenueCounters.AGGREGATE_KEYS).increment(1);
  }
}
