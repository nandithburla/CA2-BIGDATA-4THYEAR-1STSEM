package ca2.q1;

import org.apache.hadoop.mapreduce.Partitioner;

/** Custom Partitioner for Jobs 2 and 3: route by city (all meters of a city reach one reducer). */
public class CityPartitioner<V> extends Partitioner<MeterMonthKey, V> {
  @Override
  public int getPartition(MeterMonthKey key, V value, int numPartitions) {
    return (key.getCity().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }
}
