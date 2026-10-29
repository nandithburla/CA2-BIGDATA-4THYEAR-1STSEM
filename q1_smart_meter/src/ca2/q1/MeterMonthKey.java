package ca2.q1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/** Key of Jobs 2 and 3: (city, meter_id, month yyyy-MM). Sorted by city, meter, month. */
public class MeterMonthKey implements WritableComparable<MeterMonthKey> {
  private final Text city = new Text();
  private final Text meterId = new Text();
  private final Text month = new Text();

  public MeterMonthKey() { }

  public void set(String city, String meter, String month) {
    this.city.set(city);
    this.meterId.set(meter);
    this.month.set(month);
  }

  public Text getCity() { return city; }
  public Text getMeterId() { return meterId; }
  public Text getMonth() { return month; }

  @Override
  public void write(DataOutput out) throws IOException { city.write(out); meterId.write(out); month.write(out); }

  @Override
  public void readFields(DataInput in) throws IOException { city.readFields(in); meterId.readFields(in); month.readFields(in); }

  @Override
  public int compareTo(MeterMonthKey o) {
    int c = city.compareTo(o.city);
    if (c != 0) return c;
    c = meterId.compareTo(o.meterId);
    if (c != 0) return c;
    return month.compareTo(o.month);
  }

  @Override
  public boolean equals(Object o) { return o instanceof MeterMonthKey && compareTo((MeterMonthKey) o) == 0; }

  @Override
  public int hashCode() { return (city.hashCode() * 31 + meterId.hashCode()) * 31 + month.hashCode(); }

  @Override
  public String toString() { return city + "," + meterId + "," + month; }
}
