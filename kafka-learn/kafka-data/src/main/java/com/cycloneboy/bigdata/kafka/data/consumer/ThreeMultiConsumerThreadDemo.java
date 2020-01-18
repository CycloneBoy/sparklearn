package com.cycloneboy.bigdata.kafka.data.consumer;

import com.cycloneboy.bigdata.kafka.data.common.Constants;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 基于线程池来多线程消费消息
 *
 * <p>Create by sl on 2020-01-18 13:38
 */
@Slf4j
public class ThreeMultiConsumerThreadDemo {

  public static void main(String[] args) {
    Properties properties = initConfig();

    KafkaConsumerThread kafkaConsumerThread =
        new KafkaConsumerThread(
            properties, Constants.TOPIC_GREETINGS, Runtime.getRuntime().availableProcessors());
    kafkaConsumerThread.start();
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

  /** 消费者线程池 */
  public static class KafkaConsumerThread extends Thread {
    private KafkaConsumer<String, String> kafkaConsumer;
    private ExecutorService executorService;
    private int threadNumber;

    public KafkaConsumerThread(Properties properties, String topic, int threadNumber) {
      this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
      this.kafkaConsumer.subscribe(Arrays.asList(topic));
      this.threadNumber = threadNumber;
      executorService =
          new ThreadPoolExecutor(
              threadNumber,
              threadNumber,
              0L,
              TimeUnit.MILLISECONDS,
              new ArrayBlockingQueue<>(1000),
              new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void run() {
      try {
        while (true) {
          ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

          if (!records.isEmpty()) {
            executorService.submit(new RecordHandler(records));
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        kafkaConsumer.close();
      }
    }
  }

  /** 实际的干活的线程 */
  public static class RecordHandler extends Thread {
    public final ConsumerRecords<String, String> records;

    public RecordHandler(ConsumerRecords<String, String> records) {
      this.records = records;
    }

    @Override
    public void run() {
      for (ConsumerRecord<String, String> record : records) {
        log.info(
            "消费线程: {} : {} - {}",
            Thread.currentThread().getName(),
            record.partition(),
            record.value());
      }
    }
  }
}
