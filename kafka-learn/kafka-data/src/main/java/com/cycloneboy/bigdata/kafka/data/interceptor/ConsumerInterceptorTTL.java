package com.cycloneboy.bigdata.kafka.data.interceptor;

import com.cycloneboy.bigdata.kafka.data.common.Constants;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Create by sl on 2020-01-18 20:00 */
@Slf4j
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String, String> {

  @Override
  public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
    long now = System.currentTimeMillis();
    Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashedMap();

    for (TopicPartition partition : records.partitions()) {
      List<ConsumerRecord<String, String>> tpRecords = records.records(partition);

      List<ConsumerRecord<String, String>> newTpRecord = new ArrayList<>();
      for (ConsumerRecord<String, String> record : tpRecords) {
        if (now - record.timestamp() < Constants.EXPIRE_INTERVAL) {
          newTpRecord.add(record);
        }
      }

      if (!newTpRecord.isEmpty()) {
        newRecords.put(partition, newTpRecord);
      }
    }

    return new ConsumerRecords<>(newRecords);
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    offsets.forEach(
        (topicPartition, offsetAndMetadata) ->
            log.info("消费者拦截器: {} - {}", topicPartition.partition(), offsetAndMetadata.offset()));
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {}
}
