package ca2.q1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Custom SORT comparator: (meter_id, reading_ts, payload). */
public class MeterReadingSortComparator extends WritableComparator {
  public MeterReadingSortComparator() { super(MeterReadingKey.class, true); }

  @Override
  @SuppressWarnings("rawtypes")
  public int compare(WritableComparable a, WritableComparable b) {
    MeterReadingKey x = (MeterReadingKey) a, y = (MeterReadingKey) b;
    int c = x.getMeterId().compareTo(y.getMeterId());
    if (c != 0) return c;
    c = x.getReadingTs().compareTo(y.getReadingTs());
    if (c != 0) return c;
    return x.getPayload().compareTo(y.getPayload());
  }
}
