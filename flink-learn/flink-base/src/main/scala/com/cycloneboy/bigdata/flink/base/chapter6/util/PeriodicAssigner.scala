package com.cycloneboy.bigdata.flink.base.chapter6.util

import com.cycloneboy.bigdata.flink.base.util.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 *
 * Create by  sl on 2020-04-06 15:03
 */
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {

  var bound: Long = 60 * 1000
  var maxTx: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTx - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTx = maxTx.max(element.timestamp)
    element.timestamp
  }
}
