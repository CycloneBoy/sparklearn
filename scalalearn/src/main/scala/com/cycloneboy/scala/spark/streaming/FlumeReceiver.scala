package com.cycloneboy.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * Create by  sl on 2019-11-23 14:56
 */
object FlumeReceiver {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().set("spark.streaming,stopGracefullyOnShutdown", "true").setMaster("local[*]").setAppName(getClass.getSimpleName)
    val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(1))

    val stream = FlumeUtils.createPollingStream(sparkStreamingContext, "localhost", 44444)
    //   val lines = FlumeUtils.createStream(sparkStreamingContext,hostName,port)

    val mappedlines = stream.map {
      sparkFlumeEvent =>
        val event = sparkFlumeEvent.event
        println("Value of event " + event)
        println("Value of event Header " + event.getHeaders)
        println("Value of event Schema " + event.getSchema)
        val messageBody = new String(event.getBody.array())
        println("Value of event Body " + messageBody)
        messageBody
    }.print()


    stream.count().map(cnt => "Received " + cnt + " flume events.").print()
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }


}