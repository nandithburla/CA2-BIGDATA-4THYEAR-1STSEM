package ca2.q1;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * DELIBERATELY WRONG combiner (assignment 1(d)). It collapses the partial results of one mapper into a single
 * "average" record (count = 1). The reducer then averages the per-mapper averages, i.e. every mapper gets the same
 * weight regardless of how many readings it saw:  mean(mean_1, mean_2) != mean(all readings)  unless n_1 == n_2.
 * Also, Hadoop may run a combiner zero or several times, so a combiner that is not associative gives results that
 * depend on spills/splits. Used ONLY by the "wrong" experiment (-Dq1 --combiner wrong).
 */
public class WrongAverageCombiner extends Reducer<MeterMonthKey, StatsWritable, MeterMonthKey, StatsWritable> {
  private final StatsWritable out = new StatsWritable();

  @Override
  protected void reduce(MeterMonthKey key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
    long n = 0;
    double sum = 0, min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
    for (StatsWritable v : values) {
      n += v.getCount(); sum += v.getSum();
      min = Math.min(min, v.getMin()); max = Math.max(max, v.getMax());
    }
    double avg = sum / n;                       // per-mapper average ...
    out.set(1, avg, avg * avg, min, max);       // ... emitted as if it were ONE reading  (the bug)
    context.write(key, out);
  }
}
