package com.cycloneboy.bigdata.flink.datastreaming

import java.beans.Transient
import java.lang

import com.cycloneboy.bigdata.flink.model.{Alert, Transaction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 *
 * Create by  sl on 2020-04-04 11:23
 */
object FraudDetector {

  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long = 60 * 1000L
}


@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[String, Transaction, Alert] {

  @Transient private var flagState: ValueState[java.lang.Boolean] = _
  @Transient private var timerState: ValueState[java.lang.Long] = _


  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)

  }

  //  @throws[Exception]
  //  def processElement(
  //                      transaction: Transaction,
  //                      context: KeyedProcessFunction[String, Transaction, Alert]#Context,
  //                      collector: Collector[Alert]): Unit = {
  //
  //    collector.collect(Alert(transaction.accountId))
  //  }

  override def processElement(
                               transaction: Transaction,
                               ctx: KeyedProcessFunction[String, Transaction, Alert]#Context,
                               out: Collector[Alert]): Unit = {


    val lastTransactionWasSmall: lang.Boolean = flagState.value()

    if (lastTransactionWasSmall != null) {
      if (transaction.amount > FraudDetector.LARGE_AMOUNT) {

        out.collect(Alert(transaction.accountId))
      }

      cleanUp(ctx)
    }

    if (transaction.amount < FraudDetector.SMALL_AMOUNT) {
      flagState.update(true)

      // 设置一个定时器和state
      val timer: Long = ctx.timerService().currentProcessingTime() + FraudDetector.ONE_MINUTE
      ctx.timerService().registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }

  }


  override def close(): Unit = super.close()

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Transaction, Alert]#OnTimerContext, out: Collector[Alert]): Unit = {
    timerState.clear()
    flagState.clear()
  }

  private def cleanUp(ctx: KeyedProcessFunction[String, Transaction, Alert]#Context): Unit = {
    val timer: lang.Long = timerState.value()
    ctx.timerService().deleteProcessingTimeTimer(timer)

    timerState.clear()
    flagState.clear()
  }


}

