package ca2.q4;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Combiner of Job 2: element-wise sum of the stage vectors (associative, commutative, same input/output type). */
public class FunnelCombiner extends Reducer<Text, StageCounts, Text, StageCounts> {
  private final StageCounts sum = new StageCounts();

  @Override
  protected void reduce(Text key, Iterable<StageCounts> values, Context ctx) throws IOException, InterruptedException {
    sum.clear();
    for (StageCounts s : values) sum.add(s);
    ctx.write(key, sum);
  }
}
