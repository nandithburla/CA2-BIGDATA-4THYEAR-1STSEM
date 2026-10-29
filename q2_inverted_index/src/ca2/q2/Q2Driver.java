package ca2.q2;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Q2 - positional inverted index, TF-IDF and phrase search. ONE driver (Tool + ToolRunner), three chained jobs:
 *   Job 1  map-only: validate lines, count N documents with a custom counter (DocCounters.DOCS)
 *   Job 2  index: N is passed through the Configuration (q2.num.docs); stop-words from the Distributed Cache;
 *          PostingWritable with delta-encoded positions; MultipleOutputs corpus_stop / rare
 *   Job 3  phrase search on the index only: -Dphrase="big data analytics"
 *
 * hadoop jar q2_inverted_index.jar ca2.q2.Q2Driver [-D k=v] --input CORPUS_DIR --stopwords FILE --output DIR
 *        [--stage all|index|phrase] [--reducers N] [--delta on|off|fixed] [--index-dir DIR] [--keep-intermediate]
 *   -Dphrase="w1 w2 [w3]"     -Dq2.positions=kept|raw     -Dq2.corpusstop.ratio=0.4     -Dq2.phrase.main.only=true
 *   -Dca2.metrics.dir=LOCALDIR  (counters_<tag>.tsv)
 */
public class Q2Driver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new Q2Driver(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    String input = null, stopwords = null, output = null, stage = "all", delta = "on", indexDir = null;
    int reducers = 4;
    boolean keep = false;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--input": input = args[++i]; break;
        case "--stopwords": stopwords = args[++i]; break;
        case "--output": output = args[++i]; break;
        case "--stage": stage = args[++i]; break;
        case "--reducers": reducers = Integer.parseInt(args[++i]); break;
        case "--delta": delta = args[++i]; break;
        case "--index-dir": indexDir = args[++i]; break;
        case "--keep-intermediate": keep = true; break;
        default: System.err.println("Unknown argument: " + args[i]); return usage();
      }
    }
    if (!stage.matches("all|index|phrase") || !delta.matches("on|off|fixed") || output == null) return usage();
    boolean doIndex = !stage.equals("phrase"), doPhrase = !stage.equals("index");
    if (doIndex && (input == null || stopwords == null)) { System.err.println("--input and --stopwords are required to build the index"); return usage(); }
    if (indexDir == null) indexDir = output + "/index";
    String phraseDir = output + "/phrase", docsDir = output + "/docs_tmp";

    Configuration conf = getConf();
    String metrics = conf.get("ca2.metrics.dir", "");
    conf.set("q2.encoding", delta.equals("on") ? "delta" : delta.equals("off") ? "vint" : "fixed");
    String phrase = conf.get("phrase", "");
    if (doPhrase) {
      List<String> words = Tokenizer.split(phrase);
      if (words.size() < 2 || words.size() > 3) {
        System.err.println("ERROR: -Dphrase must contain 2 or 3 words (got \"" + phrase + "\")");
        return 2;
      }
      Set<String> stop = stopwords != null ? readStopwords(conf, new Path(stopwords)) : new HashSet<>();
      for (String w : words) {
        if (w.length() < 2 || stop.contains(w)) {
          System.err.println("ERROR: the phrase word \"" + w + "\" is a stop-word / too short, so it is not in the index and the phrase can never match.");
          return 2;
        }
      }
    }
    System.out.println("Q2 driver: stage=" + stage + " delta=" + delta + " reducers=" + reducers + " positions=" + conf.get("q2.positions", "kept")
        + " input=" + input + " output=" + output + " phrase=\"" + phrase + "\"");

    if (doIndex) {
      // ---------------- Job 1: count N ----------------
      Job j1 = Job.getInstance(conf, "Q2-J1-count-docs");
      j1.setJarByClass(Q2Driver.class);
      FileInputFormat.addInputPath(j1, new Path(input));
      j1.setMapperClass(DocCountMapper.class);
      j1.setNumReduceTasks(0);
      j1.setOutputKeyClass(NullWritable.class);
      j1.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(j1, new Path(docsDir));
      if (!run(j1, "count", metrics)) return fail("Job 1 (count documents) failed");
      long n = j1.getCounters().findCounter(DocCounters.DOCS).getValue();
      if (n <= 0) return fail("Job 1 counted no documents");
      System.out.println("N (DocCounters.DOCS) = " + n + "  -> passed to Job 2 as q2.num.docs");
      conf.setLong("q2.num.docs", n);

      // ---------------- Job 2: inverted index ----------------
      Job j2 = Job.getInstance(conf, "Q2-J2-index-delta-" + delta);
      j2.setJarByClass(Q2Driver.class);
      FileInputFormat.addInputPath(j2, new Path(docsDir));
      j2.addCacheFile(new URI(FileSystem.get(conf).makeQualified(new Path(stopwords)).toUri().toString() + "#stopwords.txt"));
      j2.setMapperClass(IndexMapper.class);
      j2.setMapOutputKeyClass(Text.class);
      j2.setMapOutputValueClass(PostingWritable.class);
      j2.setReducerClass(IndexReducer.class);                       // no combiner (see README)
      j2.setNumReduceTasks(reducers);
      j2.setOutputKeyClass(Text.class);
      j2.setOutputValueClass(Text.class);
      LazyOutputFormat.setOutputFormatClass(j2, TextOutputFormat.class);
      MultipleOutputs.addNamedOutput(j2, IndexReducer.CORPUS_STOP, TextOutputFormat.class, Text.class, Text.class);
      MultipleOutputs.addNamedOutput(j2, IndexReducer.RARE, TextOutputFormat.class, Text.class, Text.class);
      FileOutputFormat.setOutputPath(j2, new Path(indexDir));
      boolean ok = run(j2, "index_delta" + delta, metrics);
      if (!keep) { Path p = new Path(docsDir); p.getFileSystem(conf).delete(p, true); System.out.println("Deleted intermediate directory " + docsDir); }
      if (!ok) return fail("Job 2 (index) failed");
    }

    if (doPhrase) {
      // ---------------- Job 3: phrase search using only the index ----------------
      Job j3 = Job.getInstance(conf, "Q2-J3-phrase");
      j3.setJarByClass(Q2Driver.class);
      FileSystem fs = FileSystem.get(conf);
      boolean mainOnly = conf.getBoolean("q2.phrase.main.only", false);
      int files = 0;
      for (FileStatus st : fs.listStatus(new Path(indexDir))) {
        String nm = st.getPath().getName();
        boolean isMain = nm.startsWith("part-r-"), isSpecial = nm.startsWith("corpus_stop-r-") || nm.startsWith("rare-r-");
        if (st.isFile() && (isMain || (isSpecial && !mainOnly))) { FileInputFormat.addInputPath(j3, st.getPath()); files++; }
      }
      if (files == 0) return fail("no index files found in " + indexDir);
      j3.setMapperClass(PhraseMapper.class);
      j3.setMapOutputKeyClass(Text.class);
      j3.setMapOutputValueClass(Text.class);
      j3.setReducerClass(PhraseReducer.class);
      j3.setNumReduceTasks(1);                                       // single reducer: global ranking
      j3.setOutputKeyClass(Text.class);
      j3.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(j3, new Path(phraseDir));
      if (!run(j3, "phrase", metrics)) return fail("Job 3 (phrase search) failed");
    }
    System.out.println("Q2 driver finished successfully.");
    return 0;
  }

  private static Set<String> readStopwords(Configuration conf, Path p) throws IOException {
    Set<String> s = new HashSet<>();
    try (BufferedReader r = new BufferedReader(new java.io.InputStreamReader(p.getFileSystem(conf).open(p), java.nio.charset.StandardCharsets.UTF_8))) {
      String l;
      while ((l = r.readLine()) != null) { l = l.trim().toLowerCase(java.util.Locale.ROOT); if (!l.isEmpty()) s.add(l); }
    }
    return s;
  }

  private static boolean run(Job job, String tag, String metricsDir) throws Exception {
    boolean ok = job.waitForCompletion(true);
    Q2Util.reportJob(job, tag, metricsDir);
    return ok;
  }

  private static int fail(String msg) { System.err.println("ERROR: " + msg); return 1; }

  private static int usage() {
    System.err.println("Usage: Q2Driver [-D k=v] --input CORPUS --stopwords FILE --output DIR [--stage all|index|phrase] [--reducers N] "
        + "[--delta on|off|fixed] [--index-dir DIR] [--keep-intermediate]   (phrase: -Dphrase=\"w1 w2 w3\")");
    return 2;
  }
}
