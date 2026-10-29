package ca2.q5;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Adds "count,order,net,settled" vectors - associative and commutative, so it is a valid combiner. */
public class SummaryCombiner extends Reducer<Text, Text, Text, Text> {
  private final Text out = new Text();

  static long[] add(Iterable<Text> values) {
    long[] s = new long[4];
    for (Text t : values) {
      String[] f = t.toString().split(",");
      for (int i = 0; i < 4; i++) s[i] += Long.parseLong(f[i]);
    }
    return s;
  }

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
    long[] s = add(values);
    out.set(s[0] + "," + s[1] + "," + s[2] + "," + s[3]);
    ctx.write(key, out);
  }
}
