package com.cycloneboy.scala.spark.kafka

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer

/**
 *
 * Create by  sl on 2019-11-22 19:42
 */
object ScalaComsumerExample {

  def main(args: Array[String]): Unit = {
    val topic = "spark"
    val brokers = "localhost:9092"

    val props = new Properties()
    props.put("bootstrap", brokers)
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singleton(topic))
    val records = consumer.poll(1000)
    //    for (record <- records) {
    //      println(record)
    //    }
    consumer.close()
  }
}
