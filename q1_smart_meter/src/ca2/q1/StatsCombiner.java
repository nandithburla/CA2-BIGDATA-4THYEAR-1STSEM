package ca2.q1;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CORRECT combiner: merges partial (count, sum, sumSq, min, max). Input and output types are identical and the
 * operation is associative + commutative, so it may run zero, one or many times without changing the result.
 */
public class StatsCombiner extends Reducer<MeterMonthKey, StatsWritable, MeterMonthKey, StatsWritable> {
  private final StatsWritable acc = new StatsWritable();

  @Override
  protected void reduce(MeterMonthKey key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
    acc.reset();
    for (StatsWritable v : values) acc.merge(v);
    context.write(key, acc);
  }
}
