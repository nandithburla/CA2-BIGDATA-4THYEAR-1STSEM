package ca2.q2;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Job 2 reducer: all postings of one term arrive together.
 *   df = number of postings; tfidf = tf * log10(N/df) rounded to 4 decimals (HALF_UP);
 *   postings sorted by tfidf desc, then doc_id asc.
 *   df > 0.4 N  -> MultipleOutputs "corpus_stop"   df == 1 -> MultipleOutputs "rare"   (neither is in the main index)
 * Line format: term TAB df TAB doc:tf:tfidf:[p1,p2,...] doc:tf:tfidf:[...] ...   (positions are printed as absolute positions)
 * A Combiner is NOT applicable to this job - see README.md.
 */
public class IndexReducer extends Reducer<Text, PostingWritable, Text, Text> {
  public static final String CORPUS_STOP = "corpusstop", RARE = "rare";   // named outputs (alphanumeric); the files are corpus_stop-r-* and rare-r-*

  private static final class P { String doc; int tf; long scaled; int[] pos; }

  private MultipleOutputs<Text, Text> mos;
  private long n;
  private double stopRatio;
  private int rareDf;
  private final Text outVal = new Text();

  @Override
  protected void setup(Context ctx) {
    PostingWritable.setMode(ctx.getConfiguration().get("q2.encoding", "delta"));
    n = ctx.getConfiguration().getLong("q2.num.docs", -1);
    if (n <= 0) throw new IllegalStateException("q2.num.docs was not passed by the driver (N from the Job 1 counter)");
    stopRatio = Double.parseDouble(ctx.getConfiguration().get("q2.corpusstop.ratio", "0.4"));
    rareDf = ctx.getConfiguration().getInt("q2.rare.df", 1);
    mos = new MultipleOutputs<>(ctx);
  }

  @Override
  protected void reduce(Text term, Iterable<PostingWritable> values, Context ctx) throws IOException, InterruptedException {
    List<P> ps = new ArrayList<>();
    for (PostingWritable v : values) {              // the framework reuses 'v': copy what we keep
      P p = new P();
      p.doc = v.getDocId(); p.tf = v.getTf(); p.pos = v.positionsCopy();
      ps.add(p);
    }
    int df = ps.size();
    double idf = Math.log10((double) n / df);
    for (P p : ps) p.scaled = new BigDecimal(p.tf * idf).setScale(4, RoundingMode.HALF_UP).unscaledValue().longValue();
    ps.sort((a, b) -> a.scaled != b.scaled ? Long.compare(b.scaled, a.scaled) : a.doc.compareTo(b.doc));

    StringBuilder sb = new StringBuilder();
    sb.append(df).append('\t');
    for (int i = 0; i < ps.size(); i++) {
      P p = ps.get(i);
      if (i > 0) sb.append(' ');
      sb.append(p.doc).append(':').append(p.tf).append(':').append(BigDecimal.valueOf(p.scaled, 4).toPlainString()).append(":[");
      for (int j = 0; j < p.pos.length; j++) { if (j > 0) sb.append(','); sb.append(p.pos[j]); }
      sb.append(']');
    }
    outVal.set(sb.toString());
    ctx.getCounter(IndexCounters.TERMS_TOTAL).increment(1);
    if (df == n) ctx.getCounter(IndexCounters.TERMS_DF_EQUALS_N).increment(1);
    if ((double) df > stopRatio * n) {
      ctx.getCounter(IndexCounters.TERMS_CORPUS_STOP).increment(1);
      mos.write(CORPUS_STOP, term, outVal, "corpus_stop");
    } else if (df <= rareDf) {
      ctx.getCounter(IndexCounters.TERMS_RARE).increment(1);
      mos.write(RARE, term, outVal, "rare");
    } else {
      ctx.getCounter(IndexCounters.TERMS_MAIN_INDEX).increment(1);
      ctx.getCounter(IndexCounters.POSTINGS_MAIN_INDEX).increment(df);
      ctx.write(term, outVal);                       // main index: part-r-*
    }
  }

  @Override
  protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }
}
