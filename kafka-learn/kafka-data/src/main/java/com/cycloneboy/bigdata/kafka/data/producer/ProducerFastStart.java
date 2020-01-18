package com.cycloneboy.bigdata.kafka.data.producer;

import com.cycloneboy.bigdata.kafka.data.common.Constants;
import java.util.Properties;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/** Create by sl on 2020-01-18 13:31 */
@Slf4j
public class ProducerFastStart {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    properties.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.CLIENT_ID_DEMO);

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    ProducerRecord<String, String> record =
        new ProducerRecord<>(Constants.TOPIC_GREETINGS, "hello kafka!");

    try {
      Future<RecordMetadata> future = producer.send(record);

      RecordMetadata metadata = future.get();
      log.info("{}-{}:{}", metadata.topic(), metadata.partition(), metadata.offset());
    } catch (Exception e) {
      e.printStackTrace();
    }

    producer.send(
        record,
        (metadata, exception) -> {
          if (exception != null) {
            exception.printStackTrace();
          } else {
            log.info(
                "with callback {}-{}:{}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset());
          }
        });

    producer.close();
  }
}
