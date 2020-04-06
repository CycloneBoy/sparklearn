package com.cycloneboy.bigdata.flink.base.chapter5.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 *
 * Create by  sl on 2020-04-06 11:33
 */
class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)] {

  var subTaskIndex = 0

  override def open(parameters: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
  }

  override def close(): Unit = super.close()

  override def flatMap(value: Int, out: Collector[(Int, Int)]): Unit = {
    if (value % 2 == subTaskIndex) {
      out.collect((subTaskIndex, value))
    }
  }
}
