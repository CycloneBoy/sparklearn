package com.cycloneboy.bigdata.kafka.data.consumer;

import com.cycloneboy.bigdata.kafka.data.common.Constants;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Create by sl on 2020-01-18 13:38 */
@Slf4j
public class FirstMultiConsumerThreadDemo {

  public static void main(String[] args) {
    Properties properties = initConfig();

    int consumerThreadNumber = 4;

    for (int i = 0; i < consumerThreadNumber; i++) {
      new KafkaConsumerThread(properties, Constants.TOPIC_GREETINGS).start();
    }
  }

  private static Properties initConfig() {
    // 创建配置对象
    Properties properties = new Properties();
    // 添加配置
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID_DEMO);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    return properties;
  }

  public static class KafkaConsumerThread extends Thread {
    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaConsumerThread(Properties properties, String topic) {
      this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
      this.kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
      try {
        while (true) {
          ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

          for (ConsumerRecord<String, String> record : records) {
            log.info(
                "消费线程: {} : {} - {}",
                Thread.currentThread().getName(),
                record.partition(),
                record.value());
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        kafkaConsumer.close();
      }
    }
  }
}
