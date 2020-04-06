package com.cycloneboy.bigdata.flink.base.chapter6.util

import com.cycloneboy.bigdata.flink.base.util.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 *
 * Create by  sl on 2020-04-06 15:16
 */
class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {

  val bound: Long = 60 * 1000

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if (lastElement.id.equals("sensor_1")) {
      new Watermark(extractedTimestamp - bound)
    } else {
      null
    }

  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = element.timestamp
}
