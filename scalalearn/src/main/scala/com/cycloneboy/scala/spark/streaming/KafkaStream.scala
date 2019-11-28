package com.cycloneboy.scala.spark.streaming

import java.lang

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * Create by  sl on 2019-11-25 22:47
 */
object KafkaStream {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AdClickRealTimeStatApp").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("error")

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("hdfs://localhost:9000/user/spark/checkpoint")

    val brokerList = "localhost:9092"
    val topics = "spark"
    val consumerGroup = "sl-kafka-streaming-consumer-group"

    val kafkaParam = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
    )

    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题

    val adRealTimeLogDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topics), kafkaParam))

    var realTimeValueDStream = adRealTimeLogDStream.map(consumerRecordRDD => consumerRecordRDD.value())
    // 用于Kafka Stream的线程非安全问题，重新分区切断血统
    realTimeValueDStream = realTimeValueDStream.repartition(400)


    realTimeValueDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
