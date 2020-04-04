package com.cycloneboy.bigdata.flink.source

import java.util.Calendar

import com.cycloneboy.bigdata.flink.model.Transaction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

/**
 *
 * Create by  sl on 2020-04-04 11:19
 */
class TransactionSource extends RichParallelSourceFunction[Transaction] {

  var running: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[Transaction]): Unit = {

    val rand = new Random()

    val taskIdx: Int = this.getRuntimeContext.getIndexOfThisSubtask

    var curTransactionTemp: Seq[(String, Double)] = (1 to 10).map {
      i =>
        ("transaction_" + (taskIdx * 10 + i), rand.nextDouble() * 900)
    }


    while (running) {
      curTransactionTemp = curTransactionTemp.map {
        t => {
          if (t._2 < 400) (t._1, 0.001)
          else (t._1, t._2)
        }
      }


      val curTime: Long = Calendar.getInstance().getTimeInMillis

      curTransactionTemp.foreach(t => ctx.collect(Transaction(t._1, curTime, t._2)))

      Thread.sleep(500)
    }

  }

  override def cancel(): Unit = {
    running = false
  }

}
