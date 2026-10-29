package ca2.q5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** GROUPING comparator: join id only - one reduce() call sees the "one" record first and then all "many" records. */
public class IdGroupingComparator extends WritableComparator {
  public IdGroupingComparator() { super(TaggedKey.class, true); }

  @Override
  @SuppressWarnings("rawtypes")
  public int compare(WritableComparable a, WritableComparable b) {
    return ((TaggedKey) a).getId().compareTo(((TaggedKey) b).getId());
  }
}
