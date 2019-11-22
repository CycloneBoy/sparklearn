package com.cycloneboy.scala.spark.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 *
 * Create by  sl on 2019-11-22 19:37
 */
object ScalaProducerExample {

  def main(args: Array[String]): Unit = {

    val topic = "spark"
    val brokers = "localhost:9092"
    val props = new Properties()
    props.put("bootstrap", brokers)
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      val time = System.currentTimeMillis()
      val msg = s"$time : hello, I'm test message!"
      val record = new ProducerRecord[String, String](topic, msg, msg)
      producer.send(record)
    }

    producer.close()
  }
}
