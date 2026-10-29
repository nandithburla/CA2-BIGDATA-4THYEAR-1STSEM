package ca2.q4;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

/** Prints every counter of a finished job (@@JOB / @@COUNTER lines for run_q4.sh) and writes counters_<tag>.tsv. */
public final class Q4Util {
  private Q4Util() { }

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
