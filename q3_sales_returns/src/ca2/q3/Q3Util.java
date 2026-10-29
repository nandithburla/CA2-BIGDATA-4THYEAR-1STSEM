package ca2.q3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

/** Helpers shared by the Q3 jobs. */
public final class Q3Util {
  private Q3Util() { }

  public static BufferedReader openCacheFile(Configuration conf, URI uri) throws IOException {
    String link = uri.getFragment() != null ? uri.getFragment() : new Path(uri.getPath()).getName();
    File local = new File(link);
    if (local.exists()) return new BufferedReader(new InputStreamReader(new java.io.FileInputStream(local), StandardCharsets.UTF_8));
    Path p = new Path(uri.getPath());
    return new BufferedReader(new InputStreamReader(p.getFileSystem(conf).open(p), StandardCharsets.UTF_8));
  }

  /** Minimal CSV splitter (handles "quoted, fields" and "" escapes). */
  public static String[] splitCsv(String line) {
    List<String> out = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    boolean q = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (q) {
        if (c == '"') { if (i + 1 < line.length() && line.charAt(i + 1) == '"') { sb.append('"'); i++; } else q = false; }
        else sb.append(c);
      } else if (c == '"') q = true;
      else if (c == ',') { out.add(sb.toString()); sb.setLength(0); }
      else sb.append(c);
    }
    out.add(sb.toString());
    return out.toArray(new String[0]);
  }

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
