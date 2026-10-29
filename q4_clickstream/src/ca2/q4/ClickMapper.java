package ca2.q4;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** clicks/*.log: user_id|event_ts_epoch_ms|url|referrer|user_agent  ->  key ClickKey(user, ts, url), value NullWritable. */
public class ClickMapper extends Mapper<Object, Text, ClickKey, NullWritable> {
  private final ClickKey key = new ClickKey();

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    String line = value.toString();
    if (line.endsWith("\r")) line = line.substring(0, line.length() - 1);
    if (line.isEmpty()) return;
    ctx.getCounter(SessionCounters.LINES_READ).increment(1);
    String[] f = line.split("\\|", -1);
    if (f.length != 5) { ctx.getCounter(SessionCounters.MALFORMED_LINES).increment(1); return; }
    long ts;
    try { ts = Long.parseLong(f[1].trim()); if (ts <= 0) throw new NumberFormatException(); }
    catch (NumberFormatException e) { ctx.getCounter(SessionCounters.BAD_TIMESTAMP).increment(1); return; }
    String user = f[0].trim(), url = f[2].trim();
    if (user.isEmpty() || url.isEmpty()) { ctx.getCounter(SessionCounters.EMPTY_USER_OR_URL).increment(1); return; }
    key.set(user, ts, url);
    ctx.write(key, NullWritable.get());
    ctx.getCounter(SessionCounters.EVENTS).increment(1);
  }
}
