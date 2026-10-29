package ca2.q1;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Job 3 reducer: city,meter_id,month,readings,above_mean_plus_2sigma,percentage. */
public class OutlierReducer extends Reducer<MeterMonthKey, OutlierWritable, NullWritable, Text> {
  private final OutlierWritable acc = new OutlierWritable();
  private final Text out = new Text();

  @Override
  protected void reduce(MeterMonthKey key, Iterable<OutlierWritable> values, Context context) throws IOException, InterruptedException {
    acc.set(0, 0);
    for (OutlierWritable v : values) acc.add(v);
    double pct = acc.getTotal() == 0 ? 0.0 : 100.0 * acc.getAbove() / acc.getTotal();
    out.set(key.getCity() + "," + key.getMeterId() + "," + key.getMonth() + "," + acc.getTotal() + "," + acc.getAbove() + "," + Q1Util.fixed(pct, 4));
    context.write(NullWritable.get(), out);
    context.getCounter(OutlierCounters.METER_MONTHS).increment(1);
    context.getCounter(OutlierCounters.OUTLIER_READINGS).increment(acc.getAbove());
    if (acc.getAbove() > 0) context.getCounter(OutlierCounters.METER_MONTHS_WITH_OUTLIERS).increment(1);
  }
}
