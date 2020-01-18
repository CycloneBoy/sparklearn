package com.cycloneboy.bigdata.kafka.data.producer;

import com.cycloneboy.bigdata.kafka.data.common.Constants;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/** Create by sl on 2020-01-18 13:31 */
@Slf4j
public class ProducerInterceptor {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    properties.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.CLIENT_ID_DEMO);

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    ProducerRecord<String, String> record1 =
        new ProducerRecord<>(
            Constants.TOPIC_GREETINGS,
            0,
            System.currentTimeMillis() - Constants.EXPIRE_INTERVAL,
            null,
            "first-expire-data!");

    producer.send(record1).get();

    ProducerRecord<String, String> record2 =
        new ProducerRecord<>(
            Constants.TOPIC_GREETINGS, 0, System.currentTimeMillis(), null, "normal-data!");

    producer.send(record2).get();

    ProducerRecord<String, String> record3 =
        new ProducerRecord<>(
            Constants.TOPIC_GREETINGS,
            0,
            System.currentTimeMillis() - Constants.EXPIRE_INTERVAL,
            null,
            "last-expire-data!");

    producer.send(record3).get();

    producer.close();
  }
}
