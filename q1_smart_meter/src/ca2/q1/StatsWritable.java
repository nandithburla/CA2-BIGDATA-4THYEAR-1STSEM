package ca2.q1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Custom value Writable carrying (count, sum, sumOfSquares, min, max) - everything needed to merge partial results,
 * which is what makes a Combiner legal for mean / standard deviation.
 *
 * kWh values are stored as integer milli-kWh (kWh x 1000) inside the doubles. Integer-valued doubles below 2^53 add
 * EXACTLY and in any order, so the Combiner can regroup the additions without changing a single bit of the result
 * (output with and without the Combiner is byte-identical, see run_q1.sh).
 */
public class StatsWritable implements Writable {
  public static final double SCALE = 1000.0;

  private long count;
  private double sum;
  private double sumSq;
  private double min;
  private double max;

  public StatsWritable() { reset(); }

  public void reset() { count = 0; sum = 0; sumSq = 0; min = Double.POSITIVE_INFINITY; max = Double.NEGATIVE_INFINITY; }

  public void setSingle(double milliKwh) {
    count = 1; sum = milliKwh; sumSq = milliKwh * milliKwh; min = milliKwh; max = milliKwh;
  }

  public void set(long count, double sum, double sumSq, double min, double max) {
    this.count = count; this.sum = sum; this.sumSq = sumSq; this.min = min; this.max = max;
  }

  /** Associative and commutative merge. */
  public void merge(StatsWritable o) {
    count += o.count; sum += o.sum; sumSq += o.sumSq;
    if (o.min < min) min = o.min;
    if (o.max > max) max = o.max;
  }

  public long getCount() { return count; }
  public double getSum() { return sum; }
  public double getSumSq() { return sumSq; }
  public double getMin() { return min; }
  public double getMax() { return max; }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, count);
    out.writeDouble(sum);
    out.writeDouble(sumSq);
    out.writeDouble(min);
    out.writeDouble(max);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    count = WritableUtils.readVLong(in);
    sum = in.readDouble();
    sumSq = in.readDouble();
    min = in.readDouble();
    max = in.readDouble();
  }

  @Override
  public String toString() { return "n=" + count + " sum=" + sum + " sumSq=" + sumSq + " min=" + min + " max=" + max; }
}
