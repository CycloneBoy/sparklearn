package com.cycloneboy.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * Create by  sl on 2019-11-20 22:02
 *
 * 自定义数据源
 * 需要继承Receiver，并实现onStart、onStop方法来自定义数据源采集
 *
 */
object SocketStream {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SocketStream")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lineStream = ssc.receiverStream(new CustomerReceiver("localhost", 9999))

    val wordCount = lineStream.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
