package ca2.q1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

/** Small helpers shared by the three jobs. */
public final class Q1Util {
  private Q1Util() { }

  /** Fixed-point formatting of the exact binary value of v, rounding half-up (identical in Java and Python). */
  public static String fixed(double v, int scale) {
    return new BigDecimal(v).setScale(scale, RoundingMode.HALF_UP).toPlainString();
  }

  /** Open a distributed-cache file: first through the localized symlink in the task directory, else via the file system. */
  public static BufferedReader openCacheFile(Configuration conf, URI uri) throws IOException {
    String link = uri.getFragment() != null ? uri.getFragment() : new Path(uri.getPath()).getName();
    File local = new File(link);
    if (local.exists()) {
      return new BufferedReader(new InputStreamReader(new java.io.FileInputStream(local), StandardCharsets.UTF_8));
    }
    Path p = new Path(uri.getPath());
    FileSystem fs = p.getFileSystem(conf);
    return new BufferedReader(new InputStreamReader(fs.open(p), StandardCharsets.UTF_8));
  }

  /**
   * Print every counter of a finished job in a machine-readable form (@@COUNTER lines, used by run_q1.sh)
   * and optionally write them to a local TSV file.
   */
  public static void reportJob(Job job, String tag, String metricsDir) throws IOException, InterruptedException {
    Counters cs = job.getCounters();
    long elapsed = job.getFinishTime() - job.getStartTime();
    System.out.println("==== JOB [" + tag + "] name=" + job.getJobName() + " id=" + job.getJobID()
        + " success=" + job.isSuccessful() + " elapsed_ms=" + elapsed + " ====");
    System.out.println("@@JOB\t" + tag + "\t" + job.getJobID() + "\t" + job.isSuccessful() + "\t" + elapsed + "\t" + job.getTrackingURL());
    PrintWriter w = null;
    if (metricsDir != null && !metricsDir.isEmpty()) {
      new File(metricsDir).mkdirs();
      w = new PrintWriter(new FileOutputStream(new File(metricsDir, "counters_" + tag + ".tsv")), true);
      w.println("tag\tgroup\tcounter\tvalue");
    }
    for (CounterGroup g : cs) {
      for (Counter c : g) {
        System.out.println("@@COUNTER\t" + tag + "\t" + g.getName() + "\t" + c.getName() + "\t" + c.getValue());
        if (w != null) w.println(tag + "\t" + g.getName() + "\t" + c.getName() + "\t" + c.getValue());
      }
    }
    if (w != null) w.close();
  }
}
