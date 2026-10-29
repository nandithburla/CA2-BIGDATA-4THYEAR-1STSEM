package ca2.q3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/** Routes by (order_id, line_no) only - never by the tag - so an order line and its returns meet in one reducer. */
public class JoinPartitioner extends Partitioner<JoinKey, Text> {
  @Override
  public int getPartition(JoinKey key, Text value, int numPartitions) {
    return ((key.getOrderId().hashCode() * 31 + key.getLineNo()) & Integer.MAX_VALUE) % numPartitions;
  }
}
