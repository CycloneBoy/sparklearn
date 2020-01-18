package com.cycloneboy.bigdata.kafka.data.producer;

import com.cycloneboy.bigdata.kafka.data.common.Constants;
import com.cycloneboy.bigdata.kafka.data.model.Company;
import com.cycloneboy.bigdata.kafka.data.partition.DemoPartitoner;
import com.cycloneboy.bigdata.kafka.data.serialization.CompanySerializer;
import java.util.Properties;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 自定义序列化器
 *
 * <p>Create by sl on 2020-01-18 13:31
 */
@Slf4j
public class ProducerCompany {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class);

    properties.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.CLIENT_ID_DEMO);
    // 指定自定义分区器
    properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitoner.class);

    KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);

    Company company = Company.builder().name("hiddenkafka").address("china").build();

    ProducerRecord<String, Company> record =
        new ProducerRecord<>(Constants.TOPIC_GREETINGS, company);

    try {
      Future<RecordMetadata> future = producer.send(record);

      RecordMetadata metadata = future.get();
      log.info("{}-{}:{}", metadata.topic(), metadata.partition(), metadata.offset());
    } catch (Exception e) {
      e.printStackTrace();
    }

    producer.close();
  }
}
