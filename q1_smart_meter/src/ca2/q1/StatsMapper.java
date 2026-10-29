package ca2.q1;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Job 2 mapper: one clean reading -> ((city, meter, month), StatsWritable of a single reading). */
public class StatsMapper extends Mapper<LongWritable, Text, MeterMonthKey, StatsWritable> {
  private final MeterMonthKey outKey = new MeterMonthKey();
  private final StatsWritable outVal = new StatsWritable();

  @Override
  protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
    String[] f = line.toString().split(",", -1);       // meter_id,city,reading_ts,kwh
    if (f.length != 4 || f[2].length() < 7) { context.getCounter(StatsCounters.MALFORMED_CLEAN_LINES).increment(1); return; }
    BigDecimal milli;
    try { milli = new BigDecimal(f[3]).movePointRight(3); }
    catch (NumberFormatException e) { context.getCounter(StatsCounters.MALFORMED_CLEAN_LINES).increment(1); return; }
    BigDecimal rounded = milli.setScale(0, RoundingMode.HALF_UP);
    if (rounded.compareTo(milli) != 0) context.getCounter(StatsCounters.KWH_ROUNDED_TO_SCALE).increment(1);
    outKey.set(f[1], f[0], f[2].substring(0, 7));      // month = yyyy-MM
    outVal.setSingle(rounded.doubleValue());
    context.write(outKey, outVal);
  }
}
