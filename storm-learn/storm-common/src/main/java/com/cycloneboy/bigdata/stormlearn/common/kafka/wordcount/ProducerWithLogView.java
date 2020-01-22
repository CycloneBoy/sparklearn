package com.cycloneboy.bigdata.stormlearn.common.kafka.wordcount;

import com.cycloneboy.bigdata.stormlearn.common.log.ViewData;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/** Create by sl on 2020-01-18 13:31 */
public class ProducerWithLogView {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
    //    properties.put(
    //        ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
    //        ProducerInterceptorPrefix.class.getName()
    //            + ","
    //            + ProducerInterceptorPrefixPlus.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    ViewData viewData = new ViewData();

    int genNumber = 1000;

    Random random = new Random();
    long time = System.currentTimeMillis() - 100 * 24 * 3600 * 1000;
    int userId = 100001;
    int clickId = 1000;
    int clickType = 1000;
    int orderId = 1000;
    int orderType = 1000;
    int sendRecordNumber = 1000;

    for (int i = 0; i < sendRecordNumber; i++) {

      ViewData log = new ViewData();
      log.setTime(time + i * 60 * 1000);
      log.setUserId(userId + i);

      String sessionId = UUID.randomUUID().toString().replace("-", "").substring(0, 20);
      log.setSessionId(sessionId);
      log.setPageId(i * 100 / 7 * 3);
      log.setClickId(random.nextInt(clickId));
      log.setClickType(random.nextInt(clickType));
      log.setOrderId(random.nextInt(orderId));
      log.setOrderType(random.nextInt(orderType));

      ProducerRecord<String, String> record = new ProducerRecord<>("word-count", log.toString());

      try {
        producer.send(record);
        System.out.println("发送一条数据:" + log.toString());
        //        RecordMetadata metadata = future.get();
        //        log.info("{}-{}:{}", metadata.topic(), metadata.partition(), metadata.offset());
      } catch (Exception e) {
        e.printStackTrace();
      }

      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    producer.close();
  }
}
