package com.cycloneboy.bigdata.kafka.data.consumer;

import com.cycloneboy.bigdata.kafka.data.common.Constants;
import com.cycloneboy.bigdata.kafka.data.interceptor.ConsumerInterceptorTTL;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Create by sl on 2020-01-18 13:38 */
@Slf4j
public class ConsumerInterceptor {

  public static void main(String[] args) {

    // 创建配置对象
    Properties properties = new Properties();
    // 添加配置
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID_DEMO);
    properties.put(
        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptorTTL.class.getName());

    // 根据配置创建Kafka消费者
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    consumer.subscribe(Collections.singletonList(Constants.TOPIC_GREETINGS));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

      //      records.forEach(record -> log.info("{}", record.value()));
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.value());
      }
    }
  }
}
