package com.cycloneboy.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 *
 * Create by  sl on 2019-11-20 21:44
 *
 * RDD队列（了解）
 * 可以通过使用ssc.queueStream(queueOfRDDs)来创建DStream，
 * 每一个推送到这个队列中的RDD，都会作为一个DStream处理
 */
object RDDStream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDDStream")

    val ssc = new StreamingContext(conf, Seconds(5))

    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputQueueStream = ssc.queueStream(rddQueue, oneAtATime = false)

    val result = inputQueueStream.map((_, 1)).reduceByKey(_ + _)
    result.print()

    ssc.start()

    for (i <- 1 to 10) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }
}
