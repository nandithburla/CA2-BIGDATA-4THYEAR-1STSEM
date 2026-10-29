package ca2.q1;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/** Combiner for Job 3: adds (readings, above) pairs. */
public class OutlierCombiner extends Reducer<MeterMonthKey, OutlierWritable, MeterMonthKey, OutlierWritable> {
  private final OutlierWritable acc = new OutlierWritable();

  @Override
  protected void reduce(MeterMonthKey key, Iterable<OutlierWritable> values, Context context) throws IOException, InterruptedException {
    acc.set(0, 0);
    for (OutlierWritable v : values) acc.add(v);
    context.write(key, acc);
  }
}
