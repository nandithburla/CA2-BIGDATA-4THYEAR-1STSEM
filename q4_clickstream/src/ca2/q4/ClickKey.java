package ca2.q4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Composite key (user_id, event_ts) - plus the url, which is used ONLY as the last tie-break so that two events of one user with an
 * identical timestamp always arrive in the same (url ascending) order, whatever file/mapper they came from.
 *   Partitioner          : user_id only
 *   Sort comparator      : (user_id, event_ts, url)    [url only decides ties of the first two fields; -Dq4.tiebreak=none switches it off]
 *   Grouping comparator  : user_id only                -> one reduce() call per user, events streamed in time order
 */
public class ClickKey implements WritableComparable<ClickKey> {
  private final Text user = new Text();
  private long ts;
  private final Text url = new Text();

  public ClickKey() { }
  public void set(String user, long ts, String url) { this.user.set(user); this.ts = ts; this.url.set(url); }
  public Text getUser() { return user; }
  public long getTs() { return ts; }
  public Text getUrl() { return url; }

  @Override public void write(DataOutput out) throws IOException { user.write(out); WritableUtils.writeVLong(out, ts); url.write(out); }
  @Override public void readFields(DataInput in) throws IOException { user.readFields(in); ts = WritableUtils.readVLong(in); url.readFields(in); }

  @Override
  public int compareTo(ClickKey o) {
    int c = user.compareTo(o.user);
    if (c != 0) return c;
    c = Long.compare(ts, o.ts);
    return c != 0 ? c : url.compareTo(o.url);
  }
  @Override public boolean equals(Object o) { return o instanceof ClickKey && compareTo((ClickKey) o) == 0; }
  @Override public int hashCode() { return user.hashCode() * 31 + Long.hashCode(ts); }
  @Override public String toString() { return user + "|" + ts + "|" + url; }
}
