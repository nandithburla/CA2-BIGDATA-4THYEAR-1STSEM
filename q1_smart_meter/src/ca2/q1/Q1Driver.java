package ca2.q1;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Q1 - Smart-meter analytics. ONE driver (Tool + ToolRunner) chaining three jobs:
 *   Job 1  clean + de-duplicate   (validation counters, composite key, secondary sort)   -> clean data set
 *   Job 2  statistics             (StatsWritable + Combiner, single pass over clean data) -> count/min/max/mean/stddev
 *   Job 3  outliers               (replicated join: Job 2 output via Distributed Cache)   -> % readings > mean + 2 sigma
 *
 * Usage:
 *   hadoop jar q1.jar ca2.q1.Q1Driver [-D key=value ...] --input DIR --output DIR
 *        [--stage all|clean|stats|outliers] [--combiner on|off|wrong] [--reducers N]
 *        [--clean-dir DIR] [--stats-dir DIR] [--outliers-dir DIR] [--keep-intermediate]
 *   -Dca2.metrics.dir=LOCALDIR   writes counters_<tag>.tsv files (used by run_q1.sh)
 *   -Dq1.dup.policy=majority|smallest
 * Exit code: 0 on success, 1 if any stage fails.
 */
public class Q1Driver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new Q1Driver(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    String input = null, output = null, stage = "all", combiner = "on";
    String cleanDir = null, statsDir = null, outDir = null;
    int reducers = 4;
    boolean keep = false;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--input": input = args[++i]; break;
        case "--output": output = args[++i]; break;
        case "--stage": stage = args[++i]; break;
        case "--combiner": combiner = args[++i]; break;
        case "--reducers": reducers = Integer.parseInt(args[++i]); break;
        case "--clean-dir": cleanDir = args[++i]; break;
        case "--stats-dir": statsDir = args[++i]; break;
        case "--outliers-dir": outDir = args[++i]; break;
        case "--keep-intermediate": keep = true; break;
        default: System.err.println("Unknown argument: " + args[i]); return usage();
      }
    }
    if (!stage.matches("all|clean|stats|outliers") || !combiner.matches("on|off|wrong")) return usage();
    boolean doClean = stage.equals("all") || stage.equals("clean");
    boolean doStats = stage.equals("all") || stage.equals("stats");
    boolean doOut = stage.equals("all") || stage.equals("outliers");
    if (doClean && input == null) { System.err.println("--input is required for the clean stage"); return usage(); }
    if (cleanDir == null && output != null) cleanDir = output + "/clean";
    if (statsDir == null && output != null) statsDir = output + "/stats";
    if (outDir == null && output != null) outDir = output + "/outliers";
    if ((cleanDir == null) || (doStats && statsDir == null) || (doOut && (statsDir == null || outDir == null))) {
      System.err.println("--output (or the explicit --clean-dir/--stats-dir/--outliers-dir needed by this stage) is required");
      return usage();
    }

    Configuration conf = getConf();
    String metrics = conf.get("ca2.metrics.dir", "");
    System.out.println("Q1 driver: stage=" + stage + " combiner=" + combiner + " reducers=" + reducers
        + " input=" + input + " clean=" + cleanDir + " stats=" + statsDir + " outliers=" + outDir
        + " split.maxsize=" + conf.get("mapreduce.input.fileinputformat.split.maxsize", "(default)"));

    if (doClean) {
      Job job = Job.getInstance(conf, "Q1-J1-clean-dedup");
      job.setJarByClass(Q1Driver.class);
      FileInputFormat.addInputPath(job, new Path(input));
      job.setMapperClass(CleanMapper.class);
      job.setMapOutputKeyClass(MeterReadingKey.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setPartitionerClass(MeterPartitioner.class);
      job.setSortComparatorClass(MeterReadingSortComparator.class);
      job.setGroupingComparatorClass(MeterReadingGroupingComparator.class);
      job.setReducerClass(DedupReducer.class);
      job.setNumReduceTasks(reducers);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(job, new Path(cleanDir));
      if (!runJob(job, "clean", metrics)) return fail("Job 1 (clean/dedup) failed");
    }

    if (doStats) {
      String tag = "stats_" + (combiner.equals("on") ? "combiner" : combiner.equals("off") ? "nocombiner" : "wrongcombiner");
      Job job = Job.getInstance(conf, "Q1-J2-" + tag);
      job.setJarByClass(Q1Driver.class);
      FileInputFormat.addInputPath(job, new Path(cleanDir));
      job.setMapperClass(StatsMapper.class);
      job.setMapOutputKeyClass(MeterMonthKey.class);
      job.setMapOutputValueClass(StatsWritable.class);
      if (combiner.equals("on")) job.setCombinerClass(StatsCombiner.class);
      else if (combiner.equals("wrong")) job.setCombinerClass(WrongAverageCombiner.class);
      job.setPartitionerClass(StatsPartitioner.class);
      job.setReducerClass(StatsReducer.class);
      job.setNumReduceTasks(conf.getInt("q1.stats.reducers", reducers));
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(job, new Path(statsDir));
      if (!runJob(job, tag, metrics)) return fail("Job 2 (statistics, combiner=" + combiner + ") failed");
    }

    if (doOut) {
      Job job = Job.getInstance(conf, "Q1-J3-outliers-replicated-join");
      job.setJarByClass(Q1Driver.class);
      FileInputFormat.addInputPath(job, new Path(cleanDir));
      // side file = output of Job 2, shipped through the Distributed Cache (replicated join)
      for (URI u : cacheUris(conf, new Path(statsDir))) job.addCacheFile(u);
      job.setMapperClass(OutlierMapper.class);
      job.setMapOutputKeyClass(MeterMonthKey.class);
      job.setMapOutputValueClass(OutlierWritable.class);
      job.setCombinerClass(OutlierCombiner.class);
      job.setPartitionerClass(OutlierPartitioner.class);
      job.setReducerClass(OutlierReducer.class);
      job.setNumReduceTasks(reducers);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(job, new Path(outDir));
      if (!runJob(job, "outliers", metrics)) return fail("Job 3 (outliers) failed");
    }

    if (stage.equals("all") && !keep) {
      Path p = new Path(cleanDir);
      p.getFileSystem(conf).delete(p, true);
      System.out.println("Deleted intermediate directory " + cleanDir + " (use --keep-intermediate to keep it)");
    }
    System.out.println("Q1 driver finished successfully.");
    return 0;
  }

  /** One cache URI per non-empty part file of Job 2: <path>#stats0, #stats1 ... (symlink names in the task directory). */
  private static List<URI> cacheUris(Configuration conf, Path statsDir) throws IOException, URISyntaxException {
    FileSystem fs = statsDir.getFileSystem(conf);
    List<URI> uris = new ArrayList<>();
    int i = 0;
    for (FileStatus st : fs.listStatus(statsDir)) {
      if (!st.isFile() || !st.getPath().getName().startsWith("part-") || st.getLen() == 0) continue;
      uris.add(new URI(fs.makeQualified(st.getPath()).toUri().toString() + "#stats" + (i++)));
    }
    if (uris.isEmpty()) throw new IOException("no statistics part files found in " + statsDir);
    return uris;
  }

  private static boolean runJob(Job job, String tag, String metricsDir) throws Exception {
    boolean ok = job.waitForCompletion(true);
    Q1Util.reportJob(job, tag, metricsDir);
    return ok;
  }

  private static int fail(String msg) { System.err.println("ERROR: " + msg); return 1; }

  private static int usage() {
    System.err.println("Usage: Q1Driver [-D k=v] --input DIR --output DIR [--stage all|clean|stats|outliers] "
        + "[--combiner on|off|wrong] [--reducers N] [--clean-dir D] [--stats-dir D] [--outliers-dir D] [--keep-intermediate]");
    return 2;
  }
}
