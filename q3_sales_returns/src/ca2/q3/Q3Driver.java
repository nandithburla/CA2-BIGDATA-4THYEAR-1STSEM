package ca2.q3;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Q3 - net revenue Top-N per category after returns. ONE driver (Tool + ToolRunner), three chained jobs:
 *   Job 1  reduce-side join orders x returns on (order_id,line_no) - tagged composite key + secondary sort, MultipleInputs, MultipleOutputs orphan_returns
 *   Job 2  net revenue / units per (category, month, product): replicated join with products.csv (Distributed Cache) + Combiner
 *   Job 3  Top-N per (category, month): bounded TreeSet in the mappers (emitted in cleanup), custom CategoryPartitioner, >= 3 reducers
 *
 * hadoop jar q3_sales_returns.jar ca2.q3.Q3Driver [-D k=v] --orders F --returns F --products F --output DIR [--reducers N] [--top N] [--keep-intermediate]
 * Output directories: <output>/top5 (final), <output>/orphan_returns (final), <output>/joined and <output>/aggregates (intermediate: deleted
 * unless --keep-intermediate).   -Dca2.metrics.dir=LOCALDIR writes counters_<tag>.tsv.
 */
public class Q3Driver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new Q3Driver(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    String orders = null, returns = null, products = null, output = null;
    int reducers = 4, top = 5;
    boolean keep = false;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--orders": orders = args[++i]; break;
        case "--returns": returns = args[++i]; break;
        case "--products": products = args[++i]; break;
        case "--output": output = args[++i]; break;
        case "--reducers": reducers = Integer.parseInt(args[++i]); break;
        case "--top": top = Integer.parseInt(args[++i]); break;
        case "--keep-intermediate": keep = true; break;
        default: System.err.println("Unknown argument: " + args[i]); return usage();
      }
    }
    if (orders == null || returns == null || products == null || output == null || top < 1) return usage();
    if (reducers < 3) { System.err.println("ERROR: Job 3 needs at least 3 reducers (assignment); got " + reducers); return 2; }

    Configuration conf = getConf();
    String metrics = conf.get("ca2.metrics.dir", "");
    FileSystem fs = FileSystem.get(conf);
    Path joined = new Path(output, "joined"), aggregates = new Path(output, "aggregates"), topDir = new Path(output, "top5"), orphans = new Path(output, "orphan_returns");
    conf.setInt("q3.topn", top);
    conf.set("q3.categories", String.join(",", categories(conf, new Path(products))));
    System.out.println("Q3 driver: orders=" + orders + " returns=" + returns + " products=" + products + " output=" + output + " reducers=" + reducers + " top=" + top);
    URI productsUri = new URI(fs.makeQualified(new Path(products)).toUri().toString() + "#products.csv");

    // ---------------- Job 1: reduce-side join ----------------
    Job j1 = Job.getInstance(conf, "Q3-J1-join-orders-returns");
    j1.setJarByClass(Q3Driver.class);
    MultipleInputs.addInputPath(j1, new Path(orders), TextInputFormat.class, OrderMapper.class);
    MultipleInputs.addInputPath(j1, new Path(returns), TextInputFormat.class, ReturnMapper.class);
    j1.setMapOutputKeyClass(JoinKey.class);
    j1.setMapOutputValueClass(Text.class);
    j1.setPartitionerClass(JoinPartitioner.class);
    j1.setGroupingComparatorClass(JoinGroupingComparator.class);
    j1.setReducerClass(JoinReducer.class);
    j1.setNumReduceTasks(reducers);
    j1.setOutputKeyClass(NullWritable.class);
    j1.setOutputValueClass(Text.class);
    MultipleOutputs.addNamedOutput(j1, JoinReducer.ORPHANS, TextOutputFormat.class, NullWritable.class, Text.class);
    FileOutputFormat.setOutputPath(j1, joined);
    if (!run(j1, "join", metrics)) return fail("Job 1 (join) failed");
    // the orphan returns are a FINAL result: move them out of the intermediate directory before it is deleted
    int moved = 0;
    for (FileStatus st : fs.listStatus(joined)) {
      if (st.isFile() && st.getPath().getName().startsWith("orphan_returns-r-")) {
        fs.mkdirs(orphans);
        if (!fs.rename(st.getPath(), new Path(orphans, st.getPath().getName()))) return fail("cannot move " + st.getPath());
        moved++;
      }
    }
    System.out.println("Moved " + moved + " orphan_returns file(s) to " + orphans);

    // ---------------- Job 2: net revenue per (category, month, product) ----------------
    Job j2 = Job.getInstance(conf, "Q3-J2-net-revenue-replicated-join");
    j2.setJarByClass(Q3Driver.class);
    FileInputFormat.addInputPath(j2, joined);
    j2.addCacheFile(productsUri);
    j2.setMapperClass(RevenueMapper.class);
    j2.setMapOutputKeyClass(CatMonthProductKey.class);
    j2.setMapOutputValueClass(AggWritable.class);
    j2.setCombinerClass(AggCombiner.class);
    j2.setReducerClass(RevenueReducer.class);
    j2.setNumReduceTasks(reducers);
    j2.setOutputKeyClass(NullWritable.class);
    j2.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(j2, aggregates);
    if (!run(j2, "revenue", metrics)) return fail("Job 2 (net revenue) failed");

    // ---------------- Job 3: Top-N ----------------
    Job j3 = Job.getInstance(conf, "Q3-J3-top" + top + "-per-category-month");
    j3.setJarByClass(Q3Driver.class);
    FileInputFormat.addInputPath(j3, aggregates);
    j3.addCacheFile(productsUri);
    j3.setMapperClass(TopNMapper.class);
    j3.setMapOutputKeyClass(GroupKey.class);
    j3.setMapOutputValueClass(Candidate.class);
    j3.setPartitionerClass(CategoryPartitioner.class);
    j3.setReducerClass(TopNReducer.class);
    j3.setNumReduceTasks(reducers);
    j3.setOutputKeyClass(NullWritable.class);
    j3.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(j3, topDir);
    boolean ok = run(j3, "top", metrics);

    if (!keep) {
      fs.delete(joined, true); fs.delete(aggregates, true);
      System.out.println("Deleted intermediate directories " + joined + " and " + aggregates + " (use --keep-intermediate to keep them)");
    }
    if (!ok) return fail("Job 3 (top-N) failed");
    System.out.println("Q3 driver finished successfully.");
    return 0;
  }

  /** Sorted distinct categories of products.csv plus UNMAPPED (for the CategoryPartitioner). */
  private static Set<String> categories(Configuration conf, Path products) throws IOException {
    Set<String> cats = new TreeSet<>();
    cats.add(RevenueMapper.UNMAPPED);
    try (BufferedReader r = new BufferedReader(new java.io.InputStreamReader(products.getFileSystem(conf).open(products), java.nio.charset.StandardCharsets.UTF_8))) {
      String l;
      while ((l = r.readLine()) != null) {
        if (l.trim().isEmpty() || l.startsWith("product_id")) continue;
        String[] f = Q3Util.splitCsv(l);
        if (f.length == 3 && !f[2].trim().isEmpty()) cats.add(f[2].trim());
      }
    }
    return cats;
  }

  private static boolean run(Job job, String tag, String metricsDir) throws Exception {
    boolean ok = job.waitForCompletion(true);
    Q3Util.reportJob(job, tag, metricsDir);
    return ok;
  }

  private static int fail(String msg) { System.err.println("ERROR: " + msg); return 1; }

  private static int usage() {
    System.err.println("Usage: Q3Driver [-D k=v] --orders FILE --returns FILE --products FILE --output DIR [--reducers N(>=3)] [--top N] [--keep-intermediate]");
    return 2;
  }
}
