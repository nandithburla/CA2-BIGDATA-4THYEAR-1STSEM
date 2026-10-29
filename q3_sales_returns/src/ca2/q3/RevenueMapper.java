package ca2.q3;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job 2 mapper. products.csv (5 002 rows) is loaded in setup() from the Distributed Cache into a HashMap  -> REPLICATED join.
 * Reads the Job 1 output; the category is resolved here (unknown product -> "UNMAPPED"); the month is the month of the ORDER.
 */
public class RevenueMapper extends Mapper<Object, Text, CatMonthProductKey, AggWritable> {
  public static final String UNMAPPED = "UNMAPPED";
  private final Map<String, String> category = new HashMap<>();
  private final CatMonthProductKey key = new CatMonthProductKey();
  private final AggWritable val = new AggWritable();

  @Override
  protected void setup(Context ctx) throws IOException {
    URI[] cache = ctx.getCacheFiles();
    if (cache == null || cache.length == 0) throw new IOException("products.csv was not shipped through the Distributed Cache");
    try (BufferedReader r = Q3Util.openCacheFile(ctx.getConfiguration(), cache[0])) {
      String l;
      while ((l = r.readLine()) != null) {
        if (l.trim().isEmpty() || l.startsWith("product_id")) continue;
        String[] f = Q3Util.splitCsv(l);
        if (f.length != 3 || f[0].trim().isEmpty()) { ctx.getCounter(RevenueCounters.PRODUCTS_BAD_ROWS).increment(1); continue; }
        if (category.putIfAbsent(f[0].trim(), f[2].trim()) != null) ctx.getCounter(RevenueCounters.PRODUCTS_DUPLICATE_IDS).increment(1);
        else ctx.getCounter(RevenueCounters.PRODUCTS_LOADED).increment(1);
      }
    }
  }

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    ctx.getCounter(RevenueCounters.JOINED_LINES_READ).increment(1);
    // order_id,line_no,month,product_id,qty,returned,net_qty,unit_price_cents,net_revenue_cents
    String[] f = value.toString().split(",", -1);
    if (f.length != 9) { ctx.getCounter(RevenueCounters.BAD_JOINED_LINES).increment(1); return; }
    String cat = category.get(f[3]);
    if (cat == null) { cat = UNMAPPED; ctx.getCounter(RevenueCounters.LINES_UNMAPPED_PRODUCT).increment(1); }
    key.set(cat, f[2], f[3]);
    val.set(Long.parseLong(f[6]), Long.parseLong(f[8]));
    ctx.write(key, val);
  }
}
