package ca2.q5;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Reads the category files (category,id,customer,currency,order_amount,order_inr,net_inr,settled_inr,count,ref1,ref2) -> (category, "1,order_micro,net_micro,settled_micro"). */
public class SummaryMapper extends Mapper<Object, Text, Text, Text> {
  private final Text k = new Text(), v = new Text();

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    String[] f = value.toString().split(",", -1);
    ctx.getCounter(SummaryCounters.LINES_READ).increment(1);
    if (f.length != 11) { ctx.getCounter(SummaryCounters.BAD_LINES).increment(1); return; }
    try {
      k.set(f[0]);
      v.set("1," + Money.parseMicro(f[5]) + "," + Money.parseMicro(f[6]) + "," + Money.parseMicro(f[7]));
      ctx.write(k, v);
    } catch (RuntimeException e) {
      ctx.getCounter(SummaryCounters.BAD_LINES).increment(1);
    }
  }
}
