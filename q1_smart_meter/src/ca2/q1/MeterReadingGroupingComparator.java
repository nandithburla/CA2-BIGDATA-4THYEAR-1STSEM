package ca2.q1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Custom GROUPING comparator: (meter_id, reading_ts) - the physical reading; payload is ignored. */
public class MeterReadingGroupingComparator extends WritableComparator {
  public MeterReadingGroupingComparator() { super(MeterReadingKey.class, true); }

  @Override
  @SuppressWarnings("rawtypes")
  public int compare(WritableComparable a, WritableComparable b) {
    MeterReadingKey x = (MeterReadingKey) a, y = (MeterReadingKey) b;
    int c = x.getMeterId().compareTo(y.getMeterId());
    if (c != 0) return c;
    return x.getReadingTs().compareTo(y.getReadingTs());
  }
}
