package ca2.q5;

import java.net.URI;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Q5 - three-way payment reconciliation. ONE driver (Tool + ToolRunner), three chained jobs:
 *   Job 1  reduce-side FULL OUTER join  gateway_txns (ONE) x bank_settlements (MANY) on txn_id   -> joined_txn; orphan_settlement
 *   Job 2  reduce-side FULL OUTER join  orders (ONE) x joined_txn (MANY) on order_id + FX (Distributed Cache) + classification
 *          -> one MultipleOutputs directory per category (+ orphan_txn)
 *   Job 3  summary: count and INR total per category (Combiner, 1 reducer)
 * Both joins use TaggedKey + IdPartitioner + natural sort (id, tag) + IdGroupingComparator and STREAM the "many" side.
 *
 * hadoop jar q5_payment_reconciliation.jar ca2.q5.Q5Driver [-D k=v] --orders F --txns F --settlements F --fx F --output DIR [--reducers N] [--keep-intermediate]
 * Output: <output>/{unpaid,underpaid,overpaid,paid_not_settled,settlement_mismatch,matched,orphan_txn,orphan_settlement}/  and  <output>/summary/
 */
public class Q5Driver extends Configured implements Tool {
  static final String[] CATS = {"unpaid", "underpaid", "overpaid", "paid_not_settled", "settlement_mismatch", "matched", "orphan_txn", "orphan_settlement"};

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new Q5Driver(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    String orders = null, txns = null, settlements = null, fxFile = null, output = null;
    int reducers = 4;
    boolean keep = false;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--orders": orders = args[++i]; break;
        case "--txns": txns = args[++i]; break;
        case "--settlements": settlements = args[++i]; break;
        case "--fx": fxFile = args[++i]; break;
        case "--output": output = args[++i]; break;
        case "--reducers": reducers = Integer.parseInt(args[++i]); break;
        case "--keep-intermediate": keep = true; break;
        default: System.err.println("Unknown argument: " + args[i]); return usage();
      }
    }
    if (orders == null || txns == null || settlements == null || fxFile == null || output == null) return usage();
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    String metrics = conf.get("ca2.metrics.dir", "");
    Path out = new Path(output), work = new Path(out, "_work"), joinedTxn = new Path(work, "joined_txn");
    Path categories = new Path(work, "categories"), summary = new Path(out, "summary");
    URI fxUri = new URI(fs.makeQualified(new Path(fxFile)).toUri().toString() + "#fx_rates.csv");
    System.out.println("Q5 driver: orders=" + orders + " txns=" + txns + " settlements=" + settlements + " fx=" + fxFile + " output=" + output + " reducers=" + reducers);

    // ---------------- Job 1: transactions x settlements on txn_id ----------------
    Job j1 = Job.getInstance(conf, "Q5-J1-join-txn-settlement");
    j1.setJarByClass(Q5Driver.class);
    MultipleInputs.addInputPath(j1, new Path(txns), TextInputFormat.class, TxnMapper.class);
    MultipleInputs.addInputPath(j1, new Path(settlements), TextInputFormat.class, SettlementMapper.class);
    j1.setMapOutputKeyClass(TaggedKey.class);
    j1.setMapOutputValueClass(Text.class);
    j1.setPartitionerClass(IdPartitioner.class);
    j1.setGroupingComparatorClass(IdGroupingComparator.class);
    j1.setReducerClass(TxnJoinReducer.class);
    j1.setNumReduceTasks(reducers);
    j1.setOutputKeyClass(NullWritable.class);
    j1.setOutputValueClass(Text.class);
    LazyOutputFormat.setOutputFormatClass(j1, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(j1, TxnJoinReducer.ORPHANS, TextOutputFormat.class, NullWritable.class, Text.class);
    FileOutputFormat.setOutputPath(j1, joinedTxn);
    if (!run(j1, "txn_settlement", metrics)) return fail("Job 1 (txn x settlement) failed");

    // the orphan settlements were written into the same directory as the joined transactions: move them out before Job 2 reads that directory
    Path orphanDir = new Path(work, "orphan_settlement");
    fs.mkdirs(orphanDir);
    int moved = 0;
    for (FileStatus st : fs.listStatus(joinedTxn)) {
      if (st.isFile() && st.getPath().getName().startsWith("orphan_settlement-r-")) {
        if (!fs.rename(st.getPath(), new Path(orphanDir, st.getPath().getName()))) return fail("could not move " + st.getPath());
        moved++;
      }
    }
    System.out.println("Moved " + moved + " orphan_settlement file(s) out of the joined-transaction directory");

    // ---------------- Job 2: orders x transactions on order_id + FX + classification ----------------
    Job j2 = Job.getInstance(conf, "Q5-J2-join-order-txn-classify");
    j2.setJarByClass(Q5Driver.class);
    MultipleInputs.addInputPath(j2, new Path(orders), TextInputFormat.class, OrderMapper.class);
    MultipleInputs.addInputPath(j2, joinedTxn, TextInputFormat.class, JoinedTxnMapper.class);
    j2.addCacheFile(fxUri);
    j2.setMapOutputKeyClass(TaggedKey.class);
    j2.setMapOutputValueClass(Text.class);
    j2.setPartitionerClass(IdPartitioner.class);
    j2.setGroupingComparatorClass(IdGroupingComparator.class);
    j2.setReducerClass(ReconReducer.class);
    j2.setNumReduceTasks(reducers);
    j2.setOutputKeyClass(NullWritable.class);
    j2.setOutputValueClass(Text.class);
    LazyOutputFormat.setOutputFormatClass(j2, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(j2, ReconReducer.OUT, TextOutputFormat.class, NullWritable.class, Text.class);
    FileOutputFormat.setOutputPath(j2, categories);
    if (!run(j2, "reconcile", metrics)) return fail("Job 2 (order x txn, classification) failed");

    // ---------------- publish the category directories under <output>/ ----------------
    fs.mkdirs(out);
    int published = 0;
    for (String c : CATS) {
      Path dst = new Path(out, c);
      Path src = c.equals("orphan_settlement") ? orphanDir : new Path(categories, c);
      if (fs.exists(dst)) return fail("refusing to overwrite " + dst);
      if (!fs.exists(src) || fs.listStatus(src).length == 0) { System.out.println("category " + c + ": no rows (directory not created)"); continue; }
      if (!fs.rename(src, dst)) return fail("could not move " + src);
      published++;
    }
    System.out.println("Published " + published + " category director(ies)/file group(s) under " + out);

    // ---------------- Job 3: summary ----------------
    Job j3 = Job.getInstance(conf, "Q5-J3-category-summary");
    j3.setJarByClass(Q5Driver.class);
    int inputs = 0;
    for (String c : CATS) {
      Path p = new Path(out, c);
      if (fs.exists(p)) { FileInputFormat.addInputPath(j3, p); inputs++; }
    }
    if (inputs == 0) return fail("no category output found for the summary");
    j3.setMapperClass(SummaryMapper.class);
    j3.setMapOutputKeyClass(Text.class);
    j3.setMapOutputValueClass(Text.class);
    j3.setCombinerClass(SummaryCombiner.class);
    j3.setReducerClass(SummaryReducer.class);
    j3.setNumReduceTasks(1);
    j3.setOutputKeyClass(NullWritable.class);
    j3.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(j3, summary);
    boolean ok = run(j3, "summary", metrics);

    if (!keep) {
      fs.delete(work, true);
      System.out.println("Deleted intermediate directory " + work + " (use --keep-intermediate to keep it)");
    }
    if (!ok) return fail("Job 3 (summary) failed");
    System.out.println("Q5 driver finished successfully.");
    return 0;
  }

  private static boolean run(Job job, String tag, String metricsDir) throws Exception {
    boolean ok = job.waitForCompletion(true);
    Q5Util.reportJob(job, tag, metricsDir);
    return ok;
  }

  private static int fail(String msg) { System.err.println("ERROR: " + msg); return 1; }

  private static int usage() {
    System.err.println("Usage: Q5Driver [-D k=v] --orders F --txns F --settlements F --fx F --output DIR [--reducers N] [--keep-intermediate]");
    return 2;
  }
}
