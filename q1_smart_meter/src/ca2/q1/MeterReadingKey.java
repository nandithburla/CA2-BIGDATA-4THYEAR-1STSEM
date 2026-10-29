package ca2.q1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Composite key of Job 1: (meter_id, reading_ts, payload) where payload = "city,kwh".
 * - Partitioner uses meter_id only  -> every reading of a meter reaches the same reducer.
 * - Sort comparator uses all three  -> the copies of one reading arrive sorted by payload, so equal copies are adjacent.
 * - Grouping comparator uses (meter_id, reading_ts) -> one reduce() call per physical reading, values streamed.
 */
public class MeterReadingKey implements WritableComparable<MeterReadingKey> {
  private final Text meterId = new Text();
  private final Text readingTs = new Text();
  private final Text payload = new Text();

  public MeterReadingKey() { }

  public void set(String meter, String ts, String payload) {
    this.meterId.set(meter);
    this.readingTs.set(ts);
    this.payload.set(payload);
  }

  public Text getMeterId() { return meterId; }
  public Text getReadingTs() { return readingTs; }
  public Text getPayload() { return payload; }

  @Override
  public void write(DataOutput out) throws IOException {
    meterId.write(out);
    readingTs.write(out);
    payload.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    meterId.readFields(in);
    readingTs.readFields(in);
    payload.readFields(in);
  }

  @Override
  public int compareTo(MeterReadingKey o) {
    int c = meterId.compareTo(o.meterId);
    if (c != 0) return c;
    c = readingTs.compareTo(o.readingTs);
    if (c != 0) return c;
    return payload.compareTo(o.payload);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof MeterReadingKey && compareTo((MeterReadingKey) o) == 0;
  }

  @Override
  public int hashCode() { return meterId.hashCode() * 163 + readingTs.hashCode(); }

  @Override
  public String toString() { return meterId + "|" + readingTs + "|" + payload; }
}
