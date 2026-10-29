package ca2.q4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** SORT comparator: (user_id, event_ts); the url decides only exact ties (-Dq4.tiebreak=url|none, default url). */
public class ClickSortComparator extends WritableComparator {
  private boolean tieUrl = true;

  public ClickSortComparator() { super(ClickKey.class, true); }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) tieUrl = !"none".equals(conf.get("q4.tiebreak", "url"));
  }

  @Override
  @SuppressWarnings("rawtypes")
  public int compare(WritableComparable a, WritableComparable b) {
    ClickKey x = (ClickKey) a, y = (ClickKey) b;
    int c = x.getUser().compareTo(y.getUser());
    if (c != 0) return c;
    c = Long.compare(x.getTs(), y.getTs());
    if (c != 0 || !tieUrl) return c;
    return x.getUrl().compareTo(y.getUrl());
  }
}
