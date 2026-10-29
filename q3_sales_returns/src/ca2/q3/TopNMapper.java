package ca2.q3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job 3 mapper. Keeps, for every (category, month) group, at most N candidates in a TreeSet ordered by the strict total order
 * Candidate.BEST_FIRST (evicting the worst when the set grows beyond N) -> memory O(groups x N), independent of the number of rows.
 * The survivors are emitted in cleanup().  Input line: category,month,product_id,net_units,net_revenue
 */
public class TopNMapper extends Mapper<Object, Text, GroupKey, Candidate> {
  private int n;
  private final Map<String, TreeSet<Candidate>> groups = new HashMap<>();

  @Override
  protected void setup(Context ctx) { n = ctx.getConfiguration().getInt("q3.topn", 5); }

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    String[] f = value.toString().split(",", -1);
    if (f.length != 5) return;
    ctx.getCounter(TopNCounters.CANDIDATES_SEEN).increment(1);
    TreeSet<Candidate> set = groups.computeIfAbsent(f[0] + "," + f[1], k -> new TreeSet<>(Candidate.BEST_FIRST));
    set.add(new Candidate(f[2], Long.parseLong(f[3]), Money.parseCents(f[4])));
    if (set.size() > n) { set.pollLast(); ctx.getCounter(TopNCounters.CANDIDATES_EVICTED_IN_MAPPER).increment(1); }
  }

  @Override
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    GroupKey k = new GroupKey();
    for (Map.Entry<String, TreeSet<Candidate>> e : groups.entrySet()) {
      int comma = e.getKey().indexOf(',');
      k.set(e.getKey().substring(0, comma), e.getKey().substring(comma + 1));
      for (Candidate c : e.getValue()) { ctx.write(k, c); ctx.getCounter(TopNCounters.CANDIDATES_EMITTED).increment(1); }
    }
  }
}
