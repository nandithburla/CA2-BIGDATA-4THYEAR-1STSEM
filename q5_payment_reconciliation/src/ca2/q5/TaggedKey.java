package ca2.q5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Tagged composite key (join id, tag). The tag decides the order INSIDE a group: tag 0 = the "one" side (transaction / order) always arrives
 * first, tag 1 = the "many" side (settlements / transactions) follows and is streamed.
 *   Partitioner : id only      Sort comparator : (id, tag)      Grouping comparator : id only
 */
public class TaggedKey implements WritableComparable<TaggedKey> {
  public static final int ONE = 0, MANY = 1;
  private final Text id = new Text();
  private int tag;

  public TaggedKey() { }
  public void set(String id, int tag) { this.id.set(id); this.tag = tag; }
  public Text getId() { return id; }
  public int getTag() { return tag; }

  @Override public void write(DataOutput out) throws IOException { id.write(out); out.writeByte(tag); }
  @Override public void readFields(DataInput in) throws IOException { id.readFields(in); tag = in.readByte(); }
  @Override public int compareTo(TaggedKey o) { int c = id.compareTo(o.id); return c != 0 ? c : Integer.compare(tag, o.tag); }
  @Override public boolean equals(Object o) { return o instanceof TaggedKey && compareTo((TaggedKey) o) == 0; }
  @Override public int hashCode() { return id.hashCode(); }
  @Override public String toString() { return id + "|" + tag; }
}
