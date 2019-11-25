package com.cycloneboy.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * Create by  sl on 2019-11-25 12:13
 *
 * updateStateByKey 操作，要求必须开启 Checkpoint 机制。
 *
 *
 */
object UpdateStateByKeyWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Wordcount")


    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("error")

    ssc.checkpoint("hdfs://localhost:9000/user/spark/checkpoint/wordcount_checkpoint")

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))

    val wordCount = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue = state.getOrElse(0)
      for (value <- values) {
        newValue += value
      }
      Option(newValue)
    })

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
