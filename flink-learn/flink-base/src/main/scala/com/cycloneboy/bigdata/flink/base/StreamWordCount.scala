package com.cycloneboy.bigdata.flink.base

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 *
 * Create by  sl on 2020-03-30 22:36
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = tool.get("host")
    val port: Int = tool.get("port").toInt

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val textDstream: DataStream[String] = env.socketTextStream(host, port)

    import org.apache.flink.api.scala._

    val dStream: DataStream[(String, Int)] = textDstream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    dStream.print()

    env.execute()
  }
}
