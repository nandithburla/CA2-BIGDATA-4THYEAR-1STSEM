package ca2.q5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/** Partitions by the join id ONLY (never by the tag) so both sides of a join meet in one reducer. */
public class IdPartitioner extends Partitioner<TaggedKey, Text> {
  @Override
  public int getPartition(TaggedKey key, Text value, int numPartitions) {
    return (key.getId().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }
}
