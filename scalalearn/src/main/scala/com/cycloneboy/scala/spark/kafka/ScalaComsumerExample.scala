package com.cycloneboy.scala.spark.kafka

import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer


/**
 *
 * Create by  sl on 2019-11-22 19:42
 */
object ScalaComsumerExample {

  /**
   * 创建消费者
   *
   * @param broker
   * @param group
   * @return
   */
  def createKafkaConsumer(broker: String, group: String): KafkaConsumer[String, String] = {

    // 创建配置对象
    val props = new Properties()
    // 添加配置
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)
    // 根据配置创建Kafka消费者
    new KafkaConsumer[String, String](props)
  }

  def main(args: Array[String]): Unit = {
    //    val topic = "spark"
    val topic = "calllog"
    val broker = "localhost:9092"

    // 创建Kafka消费者
    var consumer = createKafkaConsumer(broker, "group-1")

    consumer subscribe Collections.singleton(topic)

    var result = consumer.poll(Duration.ofSeconds(60))

    result.forEach(event => {
      println(s"header:" + event.headers())
      println(s"key: " + event.key())
      println(s"value: " + event.value())
      println(s"offset: " + event.offset())
      println(s"partition: " + event.partition())
      println(s"timestamp: " + event.timestamp())
      println(s"topic: " + event.topic())
    })




    //    val records = kafkaConsumer.poll(Seconds(10))
    //
    //
    //    kafkaConsumer.close()
  }
}
