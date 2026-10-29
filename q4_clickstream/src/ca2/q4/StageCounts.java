package ca2.q4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** Per-date funnel vector: number of sessions and how many reached stage 1..4. A plain sum, so the Combiner is safe. */
public class StageCounts implements Writable {
  final long[] v = new long[5];     // v[0] = sessions, v[k] = sessions that reached stage k
  public StageCounts() { }
  public void setSession(int stage) { java.util.Arrays.fill(v, 0); v[0] = 1; for (int k = 1; k <= stage && k <= 4; k++) v[k] = 1; }
  public void add(StageCounts o) { for (int i = 0; i < 5; i++) v[i] += o.v[i]; }
  public void clear() { java.util.Arrays.fill(v, 0); }
  @Override public void write(DataOutput out) throws IOException { for (long x : v) WritableUtils.writeVLong(out, x); }
  @Override public void readFields(DataInput in) throws IOException { for (int i = 0; i < 5; i++) v[i] = WritableUtils.readVLong(in); }
}
