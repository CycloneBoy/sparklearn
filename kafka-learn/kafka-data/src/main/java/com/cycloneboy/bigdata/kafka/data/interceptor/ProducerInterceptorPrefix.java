package com.cycloneboy.bigdata.kafka.data.interceptor;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/** Create by sl on 2020-01-18 15:38 */
@Slf4j
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {

  private volatile long sendSuccess = 0;
  private volatile long sendFailure = 0;

  public static final String PREFIX1 = "testPrefix1-";

  @Override
  public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
    String modifiedValue = PREFIX1 + record.value();

    return new ProducerRecord<>(
        record.topic(),
        record.partition(),
        record.timestamp(),
        record.key(),
        modifiedValue,
        record.headers());
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    if (exception == null) {
      sendSuccess++;
    } else {
      sendFailure++;
    }
  }

  @Override
  public void close() {
    double successRaio = (double) sendSuccess / (sendSuccess + sendFailure);
    log.info(
        "[INFO]: 发送成功率: {}% - {},{}",
        String.format("%f", successRaio * 100), sendSuccess, sendFailure);
  }

  @Override
  public void configure(Map<String, ?> configs) {}
}
