package ca2.q1;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job 3 mapper = REPLICATED (map-side) JOIN. The statistics produced by Job 2 are shipped to every mapper through the
 * Distributed Cache and loaded into a HashMap in setup(); each clean reading is compared with its meter-month
 * threshold (mean + 2 sigma) in map(). No reduce-side join is used.
 */
public class OutlierMapper extends Mapper<LongWritable, Text, MeterMonthKey, OutlierWritable> {
  private final Map<String, Double> threshold = new HashMap<>();
  private final MeterMonthKey outKey = new MeterMonthKey();
  private final OutlierWritable outVal = new OutlierWritable();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    URI[] files = context.getCacheFiles();
    if (files == null) throw new IOException("no distributed-cache files: the statistics side file is missing");
    for (URI u : files) {
      String name = u.getFragment() != null ? u.getFragment() : u.getPath();
      if (!name.startsWith("stats")) continue;
      try (BufferedReader br = Q1Util.openCacheFile(context.getConfiguration(), u)) {
        String line;
        while ((line = br.readLine()) != null) {
          String[] f = line.split(",", -1);          // city,meter,month,count,min,max,mean,stddev
          if (f.length != 8) continue;
          double mean = Double.parseDouble(f[6]);
          double std = Double.parseDouble(f[7]);
          threshold.put(f[0] + "|" + f[1] + "|" + f[2], mean + 2 * std);
          context.getCounter(OutlierCounters.STATS_ROWS_LOADED).increment(1);
        }
      }
    }
  }

  @Override
  protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
    String[] f = line.toString().split(",", -1);       // meter_id,city,reading_ts,kwh
    if (f.length != 4 || f[2].length() < 7) { context.getCounter(OutlierCounters.MALFORMED_CLEAN_LINES).increment(1); return; }
    String month = f[2].substring(0, 7);
    Double thr = threshold.get(f[1] + "|" + f[0] + "|" + month);
    if (thr == null) { context.getCounter(OutlierCounters.LOOKUP_MISS).increment(1); return; }
    double kwh;
    try { kwh = Double.parseDouble(f[3]); }
    catch (NumberFormatException e) { context.getCounter(OutlierCounters.MALFORMED_CLEAN_LINES).increment(1); return; }
    context.getCounter(OutlierCounters.READINGS_SEEN).increment(1);
    outKey.set(f[1], f[0], month);
    outVal.set(1, kwh > thr ? 1 : 0);
    context.write(outKey, outVal);
  }
}
