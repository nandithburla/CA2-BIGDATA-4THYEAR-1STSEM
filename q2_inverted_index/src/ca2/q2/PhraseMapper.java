package ca2.q2;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job 3 mapper. Reads ONLY the inverted index of Job 2 (never the raw corpus). Lines of terms that are not part of the phrase are skipped;
 * for the other lines every posting is emitted as  doc_id -> "term:p1,p2,..."  (absolute positions).
 */
public class PhraseMapper extends Mapper<Object, Text, Text, Text> {
  private final Set<String> words = new HashSet<>();
  private final Text outKey = new Text(), outVal = new Text();

  @Override
  protected void setup(Context ctx) {
    List<String> ws = Tokenizer.split(ctx.getConfiguration().get("phrase", ""));
    words.addAll(ws);
  }

  @Override
  protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
    ctx.getCounter(PhraseCounters.INDEX_LINES_READ).increment(1);
    String line = value.toString();
    int t1 = line.indexOf('\t');
    if (t1 < 0) return;
    String term = line.substring(0, t1);
    if (!words.contains(term)) return;
    ctx.getCounter(PhraseCounters.INDEX_LINES_WITH_PHRASE_WORD).increment(1);
    int t2 = line.indexOf('\t', t1 + 1);
    if (t2 < 0) return;
    for (String posting : line.substring(t2 + 1).split(" ")) {
      // doc:tf:tfidf:[p1,p2,...]
      int a = posting.indexOf(':'), b = posting.indexOf(':', a + 1), c = posting.indexOf(':', b + 1);
      if (a < 0 || b < 0 || c < 0) continue;
      String doc = posting.substring(0, a);
      String list = posting.substring(c + 2, posting.length() - 1);   // strip "[" and "]"
      outKey.set(doc);
      outVal.set(term + ":" + list);
      ctx.write(outKey, outVal);
      ctx.getCounter(PhraseCounters.POSTINGS_PARSED).increment(1);
    }
  }
}
