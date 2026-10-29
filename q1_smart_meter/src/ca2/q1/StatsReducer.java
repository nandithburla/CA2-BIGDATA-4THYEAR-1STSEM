package ca2.q1;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Job 2 reducer: merge the partial statistics of one (city, meter, month) and print
 * city,meter_id,month,count,min,max,mean,stddev   (population standard deviation, kWh).
 */
public class StatsReducer extends Reducer<MeterMonthKey, StatsWritable, NullWritable, Text> {
  private final StatsWritable acc = new StatsWritable();
  private final Text out = new Text();

  @Override
  protected void reduce(MeterMonthKey key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
    acc.reset();
    for (StatsWritable v : values) acc.merge(v);

    long n = acc.getCount();
    double mean = acc.getSum() / n;                                    // milli-kWh
    // variance = E[x^2] - E[x]^2 = (n*sumSq - sum^2) / n^2.  The numerator is computed from exact integers
    // (see StatsWritable), but the guard below is kept: floating-point error could otherwise yield a tiny negative
    // value (and sqrt(negative) = NaN) when all readings are equal.
    double variance = (n * acc.getSumSq() - acc.getSum() * acc.getSum()) / ((double) n * n);
    if (variance < 0) {
      context.getCounter(StatsCounters.NEGATIVE_VARIANCE_CLAMPED).increment(1);
      variance = 0;
    }
    double std = Math.sqrt(variance);                                  // milli-kWh
    if (std == 0) context.getCounter(StatsCounters.ZERO_SIGMA_GROUPS).increment(1);
    if (n == 1) context.getCounter(StatsCounters.SINGLE_READING_GROUPS).increment(1);
    context.getCounter(StatsCounters.STAT_GROUPS).increment(1);

    double scale = StatsWritable.SCALE;
    out.set(key.getCity() + "," + key.getMeterId() + "," + key.getMonth() + "," + n + ","
        + Q1Util.fixed(acc.getMin() / scale, 3) + "," + Q1Util.fixed(acc.getMax() / scale, 3) + ","
        + Q1Util.fixed(mean / scale, 6) + "," + Q1Util.fixed(std / scale, 6));
    context.write(NullWritable.get(), out);
  }
}
