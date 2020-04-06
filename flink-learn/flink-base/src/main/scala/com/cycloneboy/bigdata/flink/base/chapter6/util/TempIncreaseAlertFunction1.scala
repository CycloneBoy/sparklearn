package com.cycloneboy.bigdata.flink.base.chapter6.util

import com.cycloneboy.bigdata.flink.base.util.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 *
 * Create by  sl on 2020-04-06 15:59
 */
class TempIncreaseAlertFunction1 extends KeyedProcessFunction[String, SensorReading, String] {


  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    out.collect("Temperature of sensor '" + ctx.getCurrentKey +
      "' monotonically increased for 1 second.")
    // reset current timer
    currentTimer.clear()

  }

  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    val prevTemp: Double = lastTemp.value()
    lastTemp.update(value.temperature)

    val curTimerTimestamp: Long = currentTimer.value()

    if (prevTemp == 0.0) {
      // first sensor reading for this key.
      // we cannot compare it with a previous value.
    } else if (value.temperature <= prevTemp) {
      // temperature decreased. Delete current timer.
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      currentTimer.clear()
    } else if (value.temperature > prevTemp && curTimerTimestamp == 0) {
      // temperature increased and we have not set a timer yet.
      // set timer for now + 1 second
      val timerTs: Long = ctx.timerService().currentProcessingTime() + 1000
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      // remember current timer
      currentTimer.update(timerTs)
    }
  }

}
