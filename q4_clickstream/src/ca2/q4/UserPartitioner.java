package ca2.q4;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/** Custom Partitioner on user_id ONLY (never on the timestamp): all events of a user reach one reducer. */
public class UserPartitioner extends Partitioner<ClickKey, NullWritable> {
  @Override
  public int getPartition(ClickKey key, NullWritable value, int numPartitions) {
    return (key.getUser().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }
}
