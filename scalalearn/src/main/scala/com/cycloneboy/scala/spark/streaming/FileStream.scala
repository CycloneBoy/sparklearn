package com.cycloneboy.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * Create by  sl on 2019-11-20 21:29
 *
 * 文件数据源
 * 文件数据流：能够读取所有HDFS API兼容的文件系统文件，
 * 通过fileStream方法进行读取，Spark Streaming 将会监控 dataDirectory 
 * 目录并不断处理移动进来的文件，记住目前不支持嵌套目录。
 */
object FileStream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("FileStream")

    val ssc = new StreamingContext(conf, Seconds(5))

    val dirStream = ssc.textFileStream("hdfs://localhost:9000/user/sl/spark/fileStream")

    val wordCount = dirStream.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
