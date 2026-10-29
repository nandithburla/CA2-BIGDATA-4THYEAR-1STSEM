package ca2.q3;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Job 3 reducer: merges the per-mapper candidates of one (category, month) into the final Top-N (again a bounded TreeSet with the
 * strict total order) and writes: category,month,rank,product_id,product_name,net_revenue,net_units
 * product_name comes from products.csv (Distributed Cache); a product that is not in the file gets "(unknown)".
 */
public class TopNReducer extends Reducer<GroupKey, Candidate, NullWritable, Text> {
  private int n;
  private final Map<String, String> names = new HashMap<>();
  private final Text out = new Text();
  private boolean wroteAny;

  @Override
  protected void setup(Context ctx) throws IOException {
    n = ctx.getConfiguration().getInt("q3.topn", 5);
    URI[] cache = ctx.getCacheFiles();
    if (cache == null || cache.length == 0) throw new IOException("products.csv was not shipped through the Distributed Cache");
    try (BufferedReader r = Q3Util.openCacheFile(ctx.getConfiguration(), cache[0])) {
      String l;
      while ((l = r.readLine()) != null) {
        if (l.trim().isEmpty() || l.startsWith("product_id")) continue;
        String[] f = Q3Util.splitCsv(l);
        if (f.length == 3) names.putIfAbsent(f[0].trim(), f[1].trim());
      }
    }
    ctx.getCounter(TopNCounters.PRODUCTS_LOADED).increment(names.size());
  }

  @Override
  protected void reduce(GroupKey key, Iterable<Candidate> values, Context ctx) throws IOException, InterruptedException {
    TreeSet<Candidate> top = new TreeSet<>(Candidate.BEST_FIRST);
    for (Candidate c : values) {
      top.add(c.copy());                           // the framework reuses 'c'
      if (top.size() > n) top.pollLast();
    }
    ctx.getCounter(TopNCounters.GROUPS).increment(1);
    wroteAny = true;
    int rank = 0;
    Candidate prev = null;
    for (Candidate c : top) {
      rank++;
      String name = names.get(c.getProduct());
      if (name == null) { name = "(unknown)"; ctx.getCounter(TopNCounters.PRODUCT_NAME_UNKNOWN).increment(1); }
      if (prev != null && prev.getCents() == c.getCents()) ctx.getCounter(TopNCounters.TIES_ON_REVENUE).increment(1);
      out.set(key.getCategory() + "," + key.getMonth() + "," + rank + "," + c.getProduct() + "," + Money.csv(name) + "," + Money.format(c.getCents()) + "," + c.getUnits());
      ctx.write(NullWritable.get(), out);
      ctx.getCounter(TopNCounters.ROWS_WRITTEN).increment(1);
      prev = c;
    }
  }

  @Override
  protected void cleanup(Context ctx) { if (wroteAny) ctx.getCounter(TopNCounters.REDUCERS_WITH_OUTPUT).increment(1); }
}
