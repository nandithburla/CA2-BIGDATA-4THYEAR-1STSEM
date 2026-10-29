package ca2.q4;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Job 2 mapper: reads the funnel facts of Job 1 ("ist_date TAB stage") of NON-bot sessions only; emits ist_date -> StageCounts. */
public class FunnelMapper extends Mapper<Object, Text, Text, StageCounts> {
  private final Text key = new Text();
  private final StageCounts val = new StageCounts();

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    ctx.getCounter(FunnelCounters.FUNNEL_SESSIONS_READ).increment(1);
    String[] f = value.toString().split("\t");
    int stage;
    try { stage = Integer.parseInt(f[1]); if (f.length != 2 || stage < 0 || stage > 4) throw new NumberFormatException(); }
    catch (RuntimeException e) { ctx.getCounter(FunnelCounters.BAD_FUNNEL_LINES).increment(1); return; }
    key.set(f[0]);
    val.setSession(stage);
    ctx.write(key, val);
  }
}
