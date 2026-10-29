package ca2.q3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** (net units, net revenue in cents) - a plain sum, so the Combiner of Job 2 is safe. */
public class AggWritable implements Writable {
  private long units, cents;
  public AggWritable() { }
  public void set(long units, long cents) { this.units = units; this.cents = cents; }
  public void add(AggWritable o) { units += o.units; cents += o.cents; }
  public long getUnits() { return units; }
  public long getCents() { return cents; }
  @Override public void write(DataOutput out) throws IOException { WritableUtils.writeVLong(out, units); WritableUtils.writeVLong(out, cents); }
  @Override public void readFields(DataInput in) throws IOException { units = WritableUtils.readVLong(in); cents = WritableUtils.readVLong(in); }
}
