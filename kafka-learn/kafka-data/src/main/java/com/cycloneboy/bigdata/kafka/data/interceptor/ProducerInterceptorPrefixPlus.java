package com.cycloneboy.bigdata.kafka.data.interceptor;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/** Create by sl on 2020-01-18 15:38 */
@Slf4j
public class ProducerInterceptorPrefixPlus implements ProducerInterceptor<String, String> {

  public static final String PREFIX2 = "testPrefix2-";

  @Override
  public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
    String modifiedValue = PREFIX2 + record.value();

    return new ProducerRecord<>(
        record.topic(),
        record.partition(),
        record.timestamp(),
        record.key(),
        modifiedValue,
        record.headers());
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {}
}
