package com.cycloneboy.scala.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * Create by  sl on 2019-11-25 22:47
 */
object KafkaStream {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaStream")

    val ssc = new StreamingContext(conf, Seconds(5))

    // kafka相关配置
    val broker = "localhost:9092"
    val sourceTopic = Set("AdRealTimeLog")
    val targetTopic = "AdRealTimeLogOut"

    val group = "spark-streaming-ad-log"
    val zookeeperUrl = "localhost:2181"

    //消费者配置
    val kafkaParam = Map[String, String](
      "bootstrap.servers" -> broker,
      "group.id" -> group,
      "zookeeper.connect" -> zookeeperUrl
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, sourceTopic)


    ssc.start()
    ssc.awaitTermination()
  }

}
