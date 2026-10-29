package ca2.q2;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job 1 (map-only): validates every corpus line, counts the documents (DocCounters.DOCS = N) and writes the well-formed lines unchanged
 * to the intermediate directory that Job 2 reads. Malformed lines are counted, not silently lost.
 */
public class DocCountMapper extends Mapper<Object, Text, NullWritable, Text> {
  @Override
  protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
    String line = value.toString();
    if (line.endsWith("\r")) line = line.substring(0, line.length() - 1);
    if (line.isEmpty()) { ctx.getCounter(DocCounters.MALFORMED_LINES).increment(1); return; }
    String[] f = line.split("\t", 4);
    if (f.length < 3) { ctx.getCounter(DocCounters.MALFORMED_LINES).increment(1); return; }
    if (f.length == 4) ctx.getCounter(DocCounters.EXTRA_TAB_LINES).increment(1);
    if (f[0].trim().isEmpty()) { ctx.getCounter(DocCounters.EMPTY_DOC_ID).increment(1); return; }
    if (f[1].trim().isEmpty()) ctx.getCounter(DocCounters.EMPTY_TITLE_DOCS).increment(1);
    String body = f.length == 4 ? f[2] + "\t" + f[3] : f[2];
    if (body.trim().isEmpty()) ctx.getCounter(DocCounters.EMPTY_BODY_DOCS).increment(1);
    ctx.getCounter(DocCounters.DOCS).increment(1);
    ctx.write(NullWritable.get(), value);
  }
}
