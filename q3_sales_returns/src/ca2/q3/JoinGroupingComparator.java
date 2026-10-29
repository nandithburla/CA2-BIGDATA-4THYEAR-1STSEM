package ca2.q3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** GROUPING comparator: (order_id, line_no) - one reduce() call per order line, order record first, returns streamed after it. */
public class JoinGroupingComparator extends WritableComparator {
  public JoinGroupingComparator() { super(JoinKey.class, true); }

  @Override
  @SuppressWarnings("rawtypes")
  public int compare(WritableComparable a, WritableComparable b) {
    JoinKey x = (JoinKey) a, y = (JoinKey) b;
    int c = x.getOrderId().compareTo(y.getOrderId());
    return c != 0 ? c : Integer.compare(x.getLineNo(), y.getLineNo());
  }
}
