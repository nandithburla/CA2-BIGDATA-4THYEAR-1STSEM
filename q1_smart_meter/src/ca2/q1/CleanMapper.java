package ca2.q1;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Job 1 mapper - VALIDATION. Rejects (and counts, one counter per category) malformed records,
 * then emits (meter_id, reading_ts, "city,kwh") -> hash of the source file name.
 * The file hash is only used by the reducer to count duplicates that crossed input files.
 */
public class CleanMapper extends Mapper<LongWritable, Text, MeterReadingKey, IntWritable> {
  private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withResolverStyle(ResolverStyle.STRICT);
  private final MeterReadingKey outKey = new MeterReadingKey();
  private final IntWritable outVal = new IntWritable();

  @Override
  protected void setup(Context context) {
    String file = ((FileSplit) context.getInputSplit()).getPath().getName();
    outVal.set(file.hashCode());
  }

  @Override
  protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
    String[] f = line.toString().split(",", -1);
    if (f.length != 4) { context.getCounter(CleanCounters.REJ_MALFORMED_FIELDS).increment(1); return; }
    String meter = f[0].trim(), city = f[1].trim(), ts = f[2].trim(), kwh = f[3].trim();
    if (meter.isEmpty() || city.isEmpty()) { context.getCounter(CleanCounters.REJ_EMPTY_METER_OR_CITY).increment(1); return; }
    if (!validTimestamp(ts)) { context.getCounter(CleanCounters.REJ_BAD_TIMESTAMP).increment(1); return; }
    if (kwh.isEmpty()) { context.getCounter(CleanCounters.REJ_EMPTY_KWH).increment(1); return; }
    BigDecimal v;
    try { v = new BigDecimal(kwh); }
    catch (NumberFormatException e) { context.getCounter(CleanCounters.REJ_NON_NUMERIC_KWH).increment(1); return; }
    if (v.signum() < 0) { context.getCounter(CleanCounters.REJ_NEGATIVE_KWH).increment(1); return; }
    context.getCounter(CleanCounters.VALID_RECORDS).increment(1);
    outKey.set(meter, ts, city + "," + kwh);
    context.write(outKey, outVal);
  }

  static boolean validTimestamp(String ts) {
    if (ts.length() != 19) return false;
    try { LocalDateTime.parse(ts, TS); return true; }
    catch (RuntimeException e) { return false; }
  }
}
