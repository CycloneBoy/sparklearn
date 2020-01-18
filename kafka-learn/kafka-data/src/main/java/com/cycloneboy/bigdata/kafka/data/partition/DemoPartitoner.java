package com.cycloneboy.bigdata.kafka.data.partition;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

/**
 * 自定义分区器,可以选择所有的分区
 *
 * <p>Create by sl on 2020-01-18 15:32
 */
public class DemoPartitoner implements Partitioner {

  private final AtomicInteger counter = new AtomicInteger();

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();
    if (keyBytes == null) {
      return counter.getAndIncrement() % numPartitions;

    } else {
      // hash the keyBytes to choose a partition
      return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {}
}
