package com.cycloneboy.scala.spark.streaming

import java.io.ObjectOutputStream
import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * Create by  sl on 2019-11-23 13:32
 */
object FlumeStream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("FlumeStream")

    val streamCtx = new StreamingContext(conf, Seconds(2))

    var addresses = new Array[InetSocketAddress](1)
    addresses(0) = new InetSocketAddress("localhost", 4949)
    //    addresses(2) = new InetSocketAddress("localhost", 4950)

    val flumeStream = FlumeUtils.createPollingStream(streamCtx, addresses,
      StorageLevel.MEMORY_AND_DISK_2, 1000, 1)

    val outputStrem = new ObjectOutputStream(Console.out)

    printValues(flumeStream, streamCtx, outputStrem)


    streamCtx.start()

    streamCtx.awaitTermination()
  }

  def printValues(stream: DStream[SparkFlumeEvent], streamCtx: StreamingContext, outputStream: ObjectOutputStream): Unit = {

    stream.foreachRDD(foreachFunc)

    def foreachFunc = (rdd: RDD[SparkFlumeEvent]) => {
      var array = rdd.collect()
      println("----------------------Start printing Results----------------------")
      println("Total size of Events= " + array.size)

      for (flumeEvent <- array) {
        val payLoad = flumeEvent.event.getBody()
        println(new String(payLoad.array()))
      }
      println("----------------------Finished printing Results----------------------")
    }

  }

}
