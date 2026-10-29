package ca2.q3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Tagged composite key of the reduce-side join: (order_id, line_no, tag) with tag 0 = order line ("one" side), 1 = return ("many" side).
 * Natural order = order_id, line_no (numeric), tag.  So inside one (order_id, line_no) group the order line ALWAYS comes first and the
 * returns follow one by one - the reducer never buffers the many side.
 */
public class JoinKey implements WritableComparable<JoinKey> {
  public static final int ORDER = 0, RETURN = 1;
  private final Text orderId = new Text();
  private int lineNo;
  private int tag;

  public JoinKey() { }
  public void set(String orderId, int lineNo, int tag) { this.orderId.set(orderId); this.lineNo = lineNo; this.tag = tag; }
  public Text getOrderId() { return orderId; }
  public int getLineNo() { return lineNo; }
  public int getTag() { return tag; }

  @Override public void write(DataOutput out) throws IOException { orderId.write(out); WritableUtils.writeVInt(out, lineNo); out.writeByte(tag); }
  @Override public void readFields(DataInput in) throws IOException { orderId.readFields(in); lineNo = WritableUtils.readVInt(in); tag = in.readByte(); }

  @Override
  public int compareTo(JoinKey o) {
    int c = orderId.compareTo(o.orderId);
    if (c != 0) return c;
    c = Integer.compare(lineNo, o.lineNo);
    if (c != 0) return c;
    return Integer.compare(tag, o.tag);
  }
  @Override public boolean equals(Object o) { return o instanceof JoinKey && compareTo((JoinKey) o) == 0; }
  @Override public int hashCode() { return orderId.hashCode() * 31 + lineNo; }
  @Override public String toString() { return orderId + "|" + lineNo + "|" + tag; }
}
