package com.cycloneboy.bigdata.kafka.data.consumer;

import com.cycloneboy.bigdata.kafka.data.common.Constants;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 指定分区的订阅方式,不具备分区分配关系的自动调整,不会实现消费者负载均衡及故障自动转移
 *
 * <p>Create by sl on 2020-01-18 13:38
 */
@Slf4j
public class ConsumerPartition {

  public static void main(String[] args) {

    // 创建配置对象
    Properties properties = new Properties();
    // 添加配置
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID_DEMO);

    // 根据配置创建Kafka消费者
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    List<TopicPartition> partitions = new ArrayList<>();

    List<PartitionInfo> partitionInfos = consumer.partitionsFor(Constants.TOPIC_GREETINGS);
    for (PartitionInfo topicInfo : partitionInfos) {
      partitions.add(new TopicPartition(topicInfo.topic(), topicInfo.partition()));
    }

    // 指定分区的订阅方式,不具备分区分配关系的自动调整,不会实现消费者负载均衡及故障自动转移
    consumer.assign(partitions);

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

      //      records.forEach(record -> log.info("{}", record.value()));
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.value());
      }
    }
  }
}
