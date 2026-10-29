package ca2.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/** Custom Partitioner for Job 1: route by meter_id ONLY (never by timestamp or payload). */
public class MeterPartitioner extends Partitioner<MeterReadingKey, IntWritable> {
  @Override
  public int getPartition(MeterReadingKey key, IntWritable value, int numPartitions) {
    return (key.getMeterId().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }
}
