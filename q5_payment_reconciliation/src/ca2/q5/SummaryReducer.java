package ca2.q5;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** One reducer: category,count,order_amount_inr_total,net_paid_inr_total,settled_inr_total. Categories with no rows are written with zeros (cleanup). */
public class SummaryReducer extends Reducer<Text, Text, NullWritable, Text> {
  static final String[] ALL = {"UNPAID", "UNDERPAID", "OVERPAID", "PAID_NOT_SETTLED", "SETTLEMENT_MISMATCH", "MATCHED", "ORPHAN_TXN", "ORPHAN_SETTLEMENT"};
  private final Set<String> seen = new HashSet<>();
  private final Text out = new Text();

  @Override
  protected void setup(Context ctx) throws IOException, InterruptedException {
    out.set("category,count,order_amount_inr_total,net_paid_inr_total,settled_inr_total");
    ctx.write(NullWritable.get(), out);
  }

  private void line(Context ctx, String cat, long[] s) throws IOException, InterruptedException {
    out.set(cat + "," + s[0] + "," + Money.micro(s[1]) + "," + Money.micro(s[2]) + "," + Money.micro(s[3]));
    ctx.write(NullWritable.get(), out);
  }

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
    seen.add(key.toString());
    ctx.getCounter(SummaryCounters.CATEGORIES).increment(1);
    line(ctx, key.toString(), SummaryCombiner.add(values));
  }

  @Override
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    for (String c : ALL) if (!seen.contains(c)) line(ctx, c, new long[4]);
  }
}
