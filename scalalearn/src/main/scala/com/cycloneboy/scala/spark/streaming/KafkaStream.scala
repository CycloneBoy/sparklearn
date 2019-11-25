package com.cycloneboy.scala.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
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
    val sourceTopic = "AdRealTimeLog"
    val targetTopic = "AdRealTimeLogOut"

    val group = "spark-streaming-ad-log"
    val zookeeperUrl = "localhost:2181"

    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> broker, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //    val sparkStream = KafkaUtils.createDirectStream[String, String](ssc, zookeeperUrl, kafkaParam)
    )

    )
  }
}
