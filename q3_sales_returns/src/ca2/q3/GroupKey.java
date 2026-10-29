package ca2.q3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/** Job 3 key: (category, month). */
public class GroupKey implements WritableComparable<GroupKey> {
  private final Text category = new Text(), month = new Text();
  public GroupKey() { }
  public void set(String c, String m) { category.set(c); month.set(m); }
  public Text getCategory() { return category; }
  public Text getMonth() { return month; }
  @Override public void write(DataOutput out) throws IOException { category.write(out); month.write(out); }
  @Override public void readFields(DataInput in) throws IOException { category.readFields(in); month.readFields(in); }
  @Override public int compareTo(GroupKey o) { int c = category.compareTo(o.category); return c != 0 ? c : month.compareTo(o.month); }
  @Override public boolean equals(Object o) { return o instanceof GroupKey && compareTo((GroupKey) o) == 0; }
  @Override public int hashCode() { return category.hashCode() * 31 + month.hashCode(); }
  @Override public String toString() { return category + "," + month; }
}
