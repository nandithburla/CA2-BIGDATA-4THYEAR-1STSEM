package ca2.q2;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job 2 mapper: tokenises title+body as ONE stream, drops short tokens and stop-words (stopwords.txt from the Distributed Cache),
 * aggregates per document (tf: title occurrence = 3, body occurrence = 1; positions) and emits one posting per (term, document).
 */
public class IndexMapper extends Mapper<Object, Text, Text, PostingWritable> {
  private static final class Acc { int tf; int[] pos = new int[4]; int n; }
  private final Set<String> stop = new HashSet<>();
  private boolean rawPositions;
  private int titleWeight;
  private final Text outKey = new Text();
  private final PostingWritable outVal = new PostingWritable();

  @Override
  protected void setup(Context ctx) throws IOException {
    PostingWritable.setMode(ctx.getConfiguration().get("q2.encoding", "delta"));
    rawPositions = "raw".equals(ctx.getConfiguration().get("q2.positions", "kept"));
    titleWeight = ctx.getConfiguration().getInt("q2.title.weight", 3);
    URI[] cache = ctx.getCacheFiles();
    if (cache == null || cache.length == 0) throw new IOException("stopwords.txt was not shipped through the Distributed Cache");
    try (BufferedReader r = Q2Util.openCacheFile(ctx.getConfiguration(), cache[0])) {
      String l;
      while ((l = r.readLine()) != null) {
        l = l.trim().toLowerCase(java.util.Locale.ROOT);
        if (!l.isEmpty()) stop.add(l);
      }
    }
    ctx.getCounter(TokenCounters.STOPWORDS_LOADED).increment(stop.size());
  }

  @Override
  protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
    String line = value.toString();
    if (line.endsWith("\r")) line = line.substring(0, line.length() - 1);
    String[] f = line.split("\t", 4);
    if (f.length < 3) return;                       // cannot happen: Job 1 already removed malformed lines
    String body = f.length == 4 ? f[2] + "\t" + f[3] : f[2];
    long[] c = new long[5];
    List<Tokenizer.Tok> toks = Tokenizer.tokenize(f[1], body, stop, rawPositions, c);
    ctx.getCounter(TokenCounters.RAW_TOKENS).increment(c[0]);
    ctx.getCounter(TokenCounters.TOKENS_TOO_SHORT).increment(c[1]);
    ctx.getCounter(TokenCounters.TOKENS_STOPWORD).increment(c[2]);
    ctx.getCounter(TokenCounters.TOKENS_KEPT).increment(c[3]);
    ctx.getCounter(TokenCounters.TITLE_TOKENS_KEPT).increment(c[4]);
    if (toks.isEmpty()) { ctx.getCounter(TokenCounters.DOCS_WITHOUT_TOKENS).increment(1); return; }

    Map<String, Acc> perDoc = new LinkedHashMap<>();
    for (Tokenizer.Tok t : toks) {
      Acc a = perDoc.get(t.term);
      if (a == null) { a = new Acc(); perDoc.put(t.term, a); }
      a.tf += t.title ? titleWeight : 1;
      if (a.n == a.pos.length) a.pos = java.util.Arrays.copyOf(a.pos, a.n * 2);
      a.pos[a.n++] = t.pos;                         // tokens arrive in increasing position order
    }
    String docId = f[0].trim();
    for (Map.Entry<String, Acc> e : perDoc.entrySet()) {
      Acc a = e.getValue();
      outVal.set(docId, a.tf);
      for (int i = 0; i < a.n; i++) outVal.addPosition(a.pos[i]);
      outKey.set(e.getKey());
      ctx.write(outKey, outVal);
      ctx.getCounter(TokenCounters.POSTINGS_EMITTED).increment(1);
    }
  }
}
