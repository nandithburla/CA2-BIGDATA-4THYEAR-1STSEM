package ca2.q3;

import java.util.Arrays;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Custom Partitioner of Job 3: routes by CATEGORY only (all months of a category go to one reducer).
 * The driver puts the sorted list of all categories (+ UNMAPPED) into q3.categories; category i goes to reducer i mod R, which spreads the
 * 13 categories evenly and guarantees min(R, #categories) busy reducers (plain hashing could leave several reducers empty).
 */
public class CategoryPartitioner extends Partitioner<GroupKey, Candidate> implements Configurable {
  private Configuration conf;
  private String[] cats = new String[0];

  @Override public void setConf(Configuration c) {
    conf = c;
    String s = c.get("q3.categories", "");
    cats = s.isEmpty() ? new String[0] : s.split(",");
    Arrays.sort(cats);
  }
  @Override public Configuration getConf() { return conf; }

  @Override
  public int getPartition(GroupKey key, Candidate value, int numPartitions) {
    int i = Arrays.binarySearch(cats, key.getCategory().toString());
    if (i < 0) return (key.getCategory().hashCode() & Integer.MAX_VALUE) % numPartitions;
    return i % numPartitions;
  }
}
