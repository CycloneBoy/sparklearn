package com.cycloneboy.bigdata.communication.kafka;

import com.cycloneboy.bigdata.communication.hbase.HBaseDao;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Create by sl on 2019-12-04 10:49 */
@Slf4j
public class HBaseConsumer {

  /**
   * 创建消费者
   *
   * @param broker
   * @param group
   * @return
   */
  public static KafkaConsumer<String, String> createKafkaConsumer(String broker, String group) {

    // 创建配置对象
    Properties props = new Properties();
    // 添加配置
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    // 根据配置创建Kafka消费者
    return new KafkaConsumer<>(props);
  }

  public static void main(String[] args) {
    //    val topic = "spark"
    String topic = "calllog";
    String broker = "localhost:9092";

    // 创建Kafka消费者
    KafkaConsumer<String, String> consumer = HBaseConsumer.createKafkaConsumer(broker, "group-1");

    consumer.subscribe(Collections.singletonList(topic));

    HBaseDao hBaseDao = new HBaseDao();

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(100));
      for (ConsumerRecord<String, String> record : records) {
        hBaseDao.put(record.value());

        log.info(
            "topic:{} -offset: {}-timestamp:{}-value:{}",
            record.topic(),
            record.offset(),
            record.timestamp(),
            record.value());
      }
    }
  }
}
