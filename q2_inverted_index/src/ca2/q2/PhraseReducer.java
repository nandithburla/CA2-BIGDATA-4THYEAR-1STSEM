package ca2.q2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Job 3 reducer (ONE reducer, so the ranking is global): per document, the phrase w0 w1 ... occurs at every position p with
 * p in pos(w0), p+1 in pos(w1), p+2 in pos(w2). A document is ranked by the number of such p (desc), then doc_id (asc).
 * The matches are held until cleanup() (only the matching documents - a few hundred bytes each), the per-document position lists are
 * released after every document.
 */
public class PhraseReducer extends Reducer<Text, Text, Text, Text> {
  private static final class Hit { String doc; int occ; }
  private List<String> phrase;
  private final List<Hit> hits = new ArrayList<>();

  @Override
  protected void setup(Context ctx) {
    phrase = Tokenizer.split(ctx.getConfiguration().get("phrase", ""));
  }

  @Override
  protected void reduce(Text doc, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
    ctx.getCounter(PhraseCounters.DOCS_CANDIDATE).increment(1);
    Map<String, int[]> pos = new HashMap<>();
    for (Text v : values) {
      String s = v.toString();
      int c = s.indexOf(':');
      String term = s.substring(0, c);
      String[] parts = s.substring(c + 1).split(",");
      int[] p = new int[parts.length];
      for (int i = 0; i < p.length; i++) p[i] = Integer.parseInt(parts[i]);
      pos.put(term, p);
    }
    for (String w : phrase) if (!pos.containsKey(w)) { ctx.getCounter(PhraseCounters.DOCS_MISSING_A_WORD).increment(1); return; }
    int occ = 0;
    for (int start : pos.get(phrase.get(0))) {
      boolean ok = true;
      for (int i = 1; i < phrase.size() && ok; i++) ok = java.util.Arrays.binarySearch(pos.get(phrase.get(i)), start + i) >= 0;
      if (ok) occ++;
    }
    if (occ == 0) { ctx.getCounter(PhraseCounters.DOCS_WORDS_NOT_ADJACENT).increment(1); return; }
    Hit h = new Hit(); h.doc = doc.toString(); h.occ = occ; hits.add(h);
    ctx.getCounter(PhraseCounters.DOCS_MATCHED).increment(1);
    ctx.getCounter(PhraseCounters.PHRASE_OCCURRENCES).increment(occ);
  }

  @Override
  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    hits.sort((a, b) -> a.occ != b.occ ? Integer.compare(b.occ, a.occ) : a.doc.compareTo(b.doc));
    Text k = new Text(), v = new Text();
    for (Hit h : hits) { k.set(h.doc); v.set(Integer.toString(h.occ)); ctx.write(k, v); }
  }
}
