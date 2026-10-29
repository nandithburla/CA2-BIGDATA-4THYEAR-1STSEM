package ca2.q3;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/** Combiner of Job 2: sums (units, cents). Associative + commutative, input type = output type. */
public class AggCombiner extends Reducer<CatMonthProductKey, AggWritable, CatMonthProductKey, AggWritable> {
  private final AggWritable sum = new AggWritable();

  @Override
  protected void reduce(CatMonthProductKey key, Iterable<AggWritable> values, Context ctx) throws IOException, InterruptedException {
    long u = 0, c = 0;
    for (AggWritable a : values) { u += a.getUnits(); c += a.getCents(); }
    sum.set(u, c);
    ctx.write(key, sum);
  }
}
