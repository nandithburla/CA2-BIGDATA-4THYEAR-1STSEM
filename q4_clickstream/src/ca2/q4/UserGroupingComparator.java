package ca2.q4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** GROUPING comparator: user_id only - the whole history of a user is ONE reduce() call. */
public class UserGroupingComparator extends WritableComparator {
  public UserGroupingComparator() { super(ClickKey.class, true); }

  @Override
  @SuppressWarnings("rawtypes")
  public int compare(WritableComparable a, WritableComparable b) {
    return ((ClickKey) a).getUser().compareTo(((ClickKey) b).getUser());
  }
}
