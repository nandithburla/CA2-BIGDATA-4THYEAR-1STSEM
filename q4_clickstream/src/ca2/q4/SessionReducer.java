package ca2.q4;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Job 1 reducer - sessionization with a STREAMING pass over the events of one user (they arrive sorted by (event_ts, url)).
 *
 * NOTHING of the user's history is stored: the reducer keeps only the state of the CURRENT session (counters, first/last url, the funnel
 * stage reached, and the set of DISTINCT urls of the current session). That set is bounded by the number of distinct urls of one session
 * (a few thousand for the 300 000-event user) - it is not a collection of events - and is cleared when the session ends.
 *
 * New session when   gap = ts - previous_ts  is STRICTLY greater than q4.gap.ms (default 30 min)   OR   the IST calendar date changes.
 * Bot session when   events > q4.bot.events (500)   OR   events >= q4.bot.min.events (2) and average gap = (end-start)/(events-1) < 1000 ms.
 *   -> MultipleOutputs "bots", excluded from the funnel; all other sessions -> main output + one funnel fact "date TAB stage".
 * Funnel stages (greedy, in order within the session): 1 /home  2 /product/*  3 /cart  4 /checkout/success.
 */
public class SessionReducer extends Reducer<ClickKey, NullWritable, NullWritable, Text> {
  public static final String BOTS = "bots", FUNNEL = "funnel", MEMORY = "reducermemory";
  private static final long IST_OFFSET_MS = 19_800_000L, DAY_MS = 86_400_000L;
  private static final DateTimeFormatter ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx");
  private static final ZoneOffset IST = ZoneOffset.ofHoursMinutes(5, 30);

  private MultipleOutputs<NullWritable, Text> mos;
  private long gapMs;
  private int botEvents, botMinEvents;
  private final Text out = new Text();

  // state of the current session
  private String user;
  private boolean open;
  private int seq;
  private long start, prevTs, prevDay, count;
  private String entry, exit;
  private int stage;
  private final Set<String> urls = new HashSet<>();
  // memory evidence
  private long eventsStreamed, maxEventsOfOneUser, peakUsedHeap;

  @Override
  protected void setup(Context ctx) {
    gapMs = ctx.getConfiguration().getLong("q4.gap.ms", 1_800_000L);
    botEvents = ctx.getConfiguration().getInt("q4.bot.events", 500);
    botMinEvents = ctx.getConfiguration().getInt("q4.bot.min.events", 2);
    mos = new MultipleOutputs<>(ctx);
  }

  @Override
  protected void reduce(ClickKey key, Iterable<NullWritable> values, Context ctx) throws IOException, InterruptedException {
    user = key.getUser().toString();
    open = false; seq = 0;
    long userEvents = 0;
    ctx.getCounter(SessionCounters.USERS).increment(1);
    for (NullWritable ignored : values) {                  // streamed: 'key' now holds the CURRENT event (ts, url)
      long ts = key.getTs();
      String url = key.getUrl().toString();
      long day = Math.floorDiv(ts + IST_OFFSET_MS, DAY_MS);
      if (open) {
        long gap = ts - prevTs;
        if (gap == 0) ctx.getCounter(SessionCounters.IDENTICAL_TIMESTAMP_EVENTS).increment(1);
        if (gap == gapMs) ctx.getCounter(SessionCounters.GAP_EXACTLY_30_MIN).increment(1);
        if (gap > gapMs) { ctx.getCounter(SessionCounters.SPLIT_BY_GAP).increment(1); closeSession(ctx); }
        else if (day != prevDay) { ctx.getCounter(SessionCounters.SPLIT_BY_MIDNIGHT_IST).increment(1); closeSession(ctx); }
      }
      if (!open) {
        open = true; seq++; start = ts; count = 0; entry = url; stage = 0; urls.clear();
      }
      count++;
      urls.add(url);
      exit = url;
      prevTs = ts; prevDay = day;
      if (matches(stage, url)) stage++;
      userEvents++;
      if ((++eventsStreamed & 0xFFFF) == 0) sampleHeap();
    }
    if (open) closeSession(ctx);
    if (userEvents > 100_000) ctx.getCounter(SessionCounters.USERS_OVER_100K_EVENTS).increment(1);
    maxEventsOfOneUser = Math.max(maxEventsOfOneUser, userEvents);
  }

  /** stage index 0..3 is the stage we are waiting for. */
  private static boolean matches(int s, String url) {
    switch (s) {
      case 0: return url.equals("/home");
      case 1: return url.startsWith("/product/");
      case 2: return url.equals("/cart");
      case 3: return url.equals("/checkout/success");
      default: return false;
    }
  }

  private void closeSession(Context ctx) throws IOException, InterruptedException {
    long durMs = prevTs - start;
    boolean byCount = count > botEvents;
    boolean byGap = count >= botMinEvents && count > 1 && durMs < 1000L * (count - 1);   // average gap < 1 s, in integers
    String line = user + "," + seq + "," + iso(start) + "," + iso(prevTs) + "," + (durMs / 1000) + "." + String.format("%03d", durMs % 1000)
        + "," + count + "," + urls.size() + "," + csv(entry) + "," + csv(exit);
    ctx.getCounter(SessionCounters.SESSIONS).increment(1);
    if (byCount || byGap) {
      ctx.getCounter(SessionCounters.SESSIONS_BOT).increment(1);
      if (byCount) ctx.getCounter(SessionCounters.BOT_BY_EVENT_COUNT).increment(1);
      if (byGap) ctx.getCounter(SessionCounters.BOT_BY_AVG_GAP).increment(1);
      out.set(line + "," + (byCount && byGap ? "EVENTS_GT_" + botEvents + "+AVG_GAP_LT_1S" : byCount ? "EVENTS_GT_" + botEvents : "AVG_GAP_LT_1S"));
      mos.write(BOTS, NullWritable.get(), out, "bots");
    } else {
      ctx.getCounter(SessionCounters.SESSIONS_NORMAL).increment(1);
      out.set(line);
      ctx.write(NullWritable.get(), out);
      out.set(LocalDate.ofEpochDay(prevDay).toString() + "\t" + stage);
      mos.write(FUNNEL, NullWritable.get(), out, "intermediate/funnel");
    }
    open = false;
    urls.clear();
  }

  private static String iso(long ts) { return Instant.ofEpochMilli(ts).atOffset(IST).format(ISO); }

  private static String csv(String s) { return s.indexOf(',') < 0 && s.indexOf('"') < 0 ? s : '"' + s.replace("\"", "\"\"") + '"'; }

  private void sampleHeap() {
    Runtime rt = Runtime.getRuntime();
    peakUsedHeap = Math.max(peakUsedHeap, rt.totalMemory() - rt.freeMemory());
  }

  @Override
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    sampleHeap();
    Runtime rt = Runtime.getRuntime();
    out.set(ctx.getTaskAttemptID() + "\tmax_heap_mb=" + (rt.maxMemory() >> 20) + "\tpeak_used_heap_mb(sampled)=" + (peakUsedHeap >> 20)
        + "\tevents_streamed=" + eventsStreamed + "\tlargest_user_events=" + maxEventsOfOneUser
        + "\tmapreduce.reduce.memory.mb=" + ctx.getConfiguration().get("mapreduce.reduce.memory.mb", "(default)")
        + "\tmapreduce.reduce.java.opts=" + ctx.getConfiguration().get("mapreduce.reduce.java.opts", "(default)"));
    mos.write(MEMORY, NullWritable.get(), out, "reducer_memory");
    mos.close();
  }
}
