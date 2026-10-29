package ca2.q5;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * fx_rates.csv (date,currency,inr_rate) - 90 rows, loaded from the Distributed Cache. rate(currency, date) is an integer in 1e-4 INR.
 * If a date has no row, the most recent EARLIER date is used (FX_FALLBACK_PREVIOUS_DAY); if there is none, the earliest later row.
 */
public final class FxTable {
  private final Map<String, TreeMap<String, Long>> rates = new HashMap<>();

  public static FxTable load(TaskInputOutputContext<?, ?, ?, ?> ctx) throws IOException {
    URI[] cache = ctx.getCacheFiles();
    if (cache == null || cache.length == 0) throw new IOException("fx_rates.csv was not shipped through the Distributed Cache");
    FxTable t = new FxTable();
    try (BufferedReader r = Q5Util.openCacheFile(ctx.getConfiguration(), cache[0])) {
      String line;
      while ((line = r.readLine()) != null) {
        String[] f = line.trim().split(",", -1);
        if (f.length != 3 || f[0].equals("date")) continue;
        t.rates.computeIfAbsent(f[1].trim().toUpperCase(java.util.Locale.ROOT), k -> new TreeMap<>()).put(f[0].trim(), Money.rate1e4(f[2]));
        ctx.getCounter(ReconCounters.FX_ROWS_LOADED).increment(1);
      }
    }
    return t;
  }

  /** @return rate in 1e-4 INR, or -1 if the currency is unknown. */
  public long rate(String currency, String date, TaskInputOutputContext<?, ?, ?, ?> ctx) {
    TreeMap<String, Long> m = rates.get(currency.toUpperCase(java.util.Locale.ROOT));
    if (m == null || m.isEmpty()) { ctx.getCounter(ReconCounters.FX_MISSING_CURRENCY).increment(1); return -1; }
    Map.Entry<String, Long> e = m.floorEntry(date);
    if (e == null || !e.getKey().equals(date)) { ctx.getCounter(ReconCounters.FX_FALLBACK_PREVIOUS_DAY).increment(1); }
    if (e == null) e = m.firstEntry();
    if (!currency.equalsIgnoreCase("INR")) ctx.getCounter(ReconCounters.FX_NON_INR_CONVERSIONS).increment(1);
    return e.getValue();
  }
}
