package com.cycloneboy.scala.spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Create by  sl on 2019-11-16 21:56
 */
object WordCountStreaming {

  case class WordText(text: String)

  def main(args: Array[String]): Unit = {

    /**
     * SparkContext 的初始化需要一个SparkConf对象
     * SparkConf包含了Spark集群的配置的各种参数
     */
    val conf = new SparkConf()
      .setMaster("local") //启动本地化计算7
      .setAppName("WordCountStreaming") //设置本程序名称
    //    conf.set("spark.eventLog.enabled", "true")

    //Spark程序的编写都是从SparkContext开始的
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))
    //      .checkpoint("data/spark/checkpoint")

    val lines = ssc.socketTextStream("localhost", 9900)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print(5)
    wordCounts.saveAsTextFiles("data/spark/streaming/wordcount", "txt")

    ssc.start()

    ssc.awaitTermination()
  }

}
