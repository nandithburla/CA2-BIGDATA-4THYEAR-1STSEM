package ca2.q4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
 * Q4 - clickstream sessionization (secondary sort) and conversion funnel. ONE driver (Tool + ToolRunner), two chained jobs:
 *   Job 1  secondary sort: ClickKey(user,ts,url) / UserPartitioner / ClickSortComparator / UserGroupingComparator;
 *          STREAMING reducer -> sessions (main output), bots (MultipleOutputs), funnel facts (MultipleOutputs, intermediate), reducer_memory
 *   Job 2  per-IST-date funnel aggregation (Combiner) over the funnel facts of NON-bot sessions
 *
 * hadoop jar q4_clickstream.jar ca2.q4.Q4Driver [-D k=v] --input DIR --output DIR [--reducers N] [--keep-intermediate]
 *   -Dmapreduce.reduce.memory.mb=512 -Dmapreduce.reduce.java.opts=-Xmx384m    (the memory proof of the assignment)
 *   -Dq4.gap.ms=1800000  -Dq4.bot.events=500  -Dq4.bot.min.events=2  -Dq4.tiebreak=url|none  -Dca2.metrics.dir=LOCALDIR
 * Output: <output>/sessions (part-r-*, bots-r-*, reducer_memory-r-*), <output>/funnel (part-r-*); <output>/sessions/intermediate is deleted.
 */
public class Q4Driver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new Q4Driver(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    String input = null, output = null;
    int reducers = 4;
    boolean keep = false;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--input": input = args[++i]; break;
        case "--output": output = args[++i]; break;
        case "--reducers": reducers = Integer.parseInt(args[++i]); break;
        case "--keep-intermediate": keep = true; break;
        default: System.err.println("Unknown argument: " + args[i]); return usage();
      }
    }
    if (input == null || output == null) return usage();
    Configuration conf = getConf();
    String metrics = conf.get("ca2.metrics.dir", "");
    Path sessions = new Path(output, "sessions"), inter = new Path(sessions, "intermediate"), funnel = new Path(output, "funnel");
    System.out.println("Q4 driver: input=" + input + " output=" + output + " reducers=" + reducers
        + " gap.ms=" + conf.get("q4.gap.ms", "1800000") + " bot.events=" + conf.get("q4.bot.events", "500")
        + " bot.min.events=" + conf.get("q4.bot.min.events", "2") + " tiebreak=" + conf.get("q4.tiebreak", "url")
        + " reduce.memory.mb=" + conf.get("mapreduce.reduce.memory.mb", "(default)") + " reduce.java.opts=" + conf.get("mapreduce.reduce.java.opts", "(default)"));

    // ---------------- Job 1: sessionization ----------------
    Job j1 = Job.getInstance(conf, "Q4-J1-sessionize-secondary-sort");
    j1.setJarByClass(Q4Driver.class);
    FileInputFormat.addInputPath(j1, new Path(input));
    j1.setMapperClass(ClickMapper.class);
    j1.setMapOutputKeyClass(ClickKey.class);
    j1.setMapOutputValueClass(NullWritable.class);
    j1.setPartitionerClass(UserPartitioner.class);
    j1.setSortComparatorClass(ClickSortComparator.class);
    j1.setGroupingComparatorClass(UserGroupingComparator.class);
    j1.setReducerClass(SessionReducer.class);
    j1.setNumReduceTasks(reducers);
    j1.setOutputKeyClass(NullWritable.class);
    j1.setOutputValueClass(Text.class);
    LazyOutputFormat.setOutputFormatClass(j1, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(j1, SessionReducer.BOTS, TextOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(j1, SessionReducer.FUNNEL, TextOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(j1, SessionReducer.MEMORY, TextOutputFormat.class, NullWritable.class, Text.class);
    FileOutputFormat.setOutputPath(j1, sessions);
    if (!run(j1, "sessions", metrics)) return fail("Job 1 (sessionization) failed");

    // ---------------- Job 2: funnel per IST date ----------------
    Job j2 = Job.getInstance(conf, "Q4-J2-funnel-per-date");
    j2.setJarByClass(Q4Driver.class);
    FileInputFormat.addInputPath(j2, inter);
    j2.setMapperClass(FunnelMapper.class);
    j2.setMapOutputKeyClass(Text.class);
    j2.setMapOutputValueClass(StageCounts.class);
    j2.setCombinerClass(FunnelCombiner.class);
    j2.setReducerClass(FunnelReducer.class);
    j2.setNumReduceTasks(1);
    j2.setOutputKeyClass(NullWritable.class);
    j2.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(j2, funnel);
    boolean ok = run(j2, "funnel", metrics);

    if (!keep) {
      FileSystem.get(conf).delete(inter, true);
      System.out.println("Deleted intermediate directory " + inter + " (use --keep-intermediate to keep it)");
    }
    if (!ok) return fail("Job 2 (funnel) failed");
    System.out.println("Q4 driver finished successfully.");
    return 0;
  }

  private static boolean run(Job job, String tag, String metricsDir) throws Exception {
    boolean ok = job.waitForCompletion(true);
    Q4Util.reportJob(job, tag, metricsDir);
    return ok;
  }

  private static int fail(String msg) { System.err.println("ERROR: " + msg); return 1; }

  private static int usage() {
    System.err.println("Usage: Q4Driver [-D k=v] --input DIR --output DIR [--reducers N] [--keep-intermediate]");
    return 2;
  }
}
