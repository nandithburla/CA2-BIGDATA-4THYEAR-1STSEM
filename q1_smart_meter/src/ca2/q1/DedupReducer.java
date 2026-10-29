package ca2.q1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Job 1 reducer - DE-DUPLICATION (one reduce() call per (meter_id, reading_ts)).
 * Because of the secondary sort the copies arrive sorted by payload, so the reducer only needs O(1) memory:
 * it walks runs of identical payloads and remembers the best run.
 *   q1.dup.policy = majority (default): keep the payload that occurs most often (exact retransmissions win),
 *                                       ties -> smallest payload
 *   q1.dup.policy = smallest          : keep the smallest payload ("city,kwh" ascending)
 * Either way the result does not depend on the order in which map outputs arrive.
 */
public class DedupReducer extends Reducer<MeterReadingKey, IntWritable, NullWritable, Text> {
  private boolean majority = true;
  private final Text out = new Text();
  private final Text runPayload = new Text();
  private final Set<Integer> files = new HashSet<>();

  @Override
  protected void setup(Context context) {
    majority = !"smallest".equalsIgnoreCase(context.getConfiguration().get("q1.dup.policy", "majority"));
  }

  @Override
  protected void reduce(MeterReadingKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    String meter = key.getMeterId().toString();
    String ts = key.getReadingTs().toString();
    files.clear();
    long total = 0, runCount = 0, bestCount = 0;
    int distinct = 0;
    String best = null;
    String runStr = null;
    for (IntWritable fileHash : values) {
      total++;
      files.add(fileHash.get());
      Text cur = key.getPayload();            // the key object is refreshed for every value
      if (runCount == 0 || !cur.equals(runPayload)) {
        if (runCount > 0 && (best == null || (majority && runCount > bestCount))) { best = runStr; bestCount = runCount; }
        runPayload.set(cur);
        runStr = cur.toString();
        runCount = 1;
        distinct++;
      } else {
        runCount++;
      }
    }
    if (runCount > 0 && (best == null || (majority && runCount > bestCount))) { best = runStr; bestCount = runCount; }

    if (total > 1) {
      context.getCounter(CleanCounters.DUP_KEY_GROUPS).increment(1);
      context.getCounter(CleanCounters.DUP_RECORDS_DROPPED).increment(total - 1);
      if (files.size() > 1) context.getCounter(CleanCounters.DUP_GROUPS_CROSS_FILE).increment(1);
      if (distinct > 1) context.getCounter(CleanCounters.DUP_GROUPS_CONFLICTING).increment(1);
    }
    int comma = best.indexOf(',');
    out.set(meter + "," + best.substring(0, comma) + "," + ts + "," + best.substring(comma + 1));
    context.write(NullWritable.get(), out);
    context.getCounter(CleanCounters.CLEAN_RECORDS).increment(1);
  }
}
