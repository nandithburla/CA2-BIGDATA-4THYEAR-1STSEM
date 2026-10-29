package ca2.q1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** Custom value Writable for Job 3: (readings, readings above mean + 2 sigma). Summable, so a Combiner is legal. */
public class OutlierWritable implements Writable {
  private long total;
  private long above;

  public OutlierWritable() { }

  public void set(long total, long above) { this.total = total; this.above = above; }
  public void add(OutlierWritable o) { total += o.total; above += o.above; }
  public long getTotal() { return total; }
  public long getAbove() { return above; }

  @Override
  public void write(DataOutput out) throws IOException { WritableUtils.writeVLong(out, total); WritableUtils.writeVLong(out, above); }

  @Override
  public void readFields(DataInput in) throws IOException { total = WritableUtils.readVLong(in); above = WritableUtils.readVLong(in); }
}
