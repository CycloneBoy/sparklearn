package com.cycloneboy.bigdata.flink.base.chapter6.util

import com.cycloneboy.bigdata.flink.base.util.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

/**
 *
 * Create by  sl on 2020-04-06 16:35
 */
class SideOutputs1 extends ProcessFunction[SensorReading, SensorReading] {

  lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarms")

  override def processElement(value: SensorReading,
                              ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0) {
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${value.id}")
    }

    out.collect(value)
  }
}
